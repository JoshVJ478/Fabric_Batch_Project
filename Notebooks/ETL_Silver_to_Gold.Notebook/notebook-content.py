# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3ebe8585-6244-49ef-b562-aef294498060",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "ccccd7d7-1d84-4bf9-80a8-07c46acd9896",
# META       "known_lakehouses": [
# META         {
# META           "id": "3ebe8585-6244-49ef-b562-aef294498060"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Transformation

# CELL ********************

# Databricks notebook source
# MAGIC %pip install delta-spark

# COMMAND ----------
# Importar las librerías necesarias
import datetime
from pyspark.sql.functions import col, lit, coalesce, current_timestamp, date_format, max, row_number, monotonically_increasing_id, sha2, dayofweek, when, weekofyear # <-- Añadido weekofyear
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.window import Window
import sys

# Importar mssparkutils para operaciones del sistema de archivos en Fabric
from notebookutils import mssparkutils

# Importar DeltaTable para operaciones MERGE
from delta.tables import DeltaTable


# COMMAND ----------

# Definir las rutas a las capas Silver y Gold en OneLake
# Las tablas de la capa Silver están en LH_Silver.
# Las tablas de la capa Gold (Hechos y Dimensiones) están en LH_Gold.
# Asegúrate de que estos nombres de Lakehouse y la ruta ABFS sean correctos para tu entorno.
silver_lakehouse_name = "LH_Silver" # Nombre del Lakehouse para la capa Silver (Origen)
gold_lakehouse_name = "LH_Gold"   # Nombre del Lakehouse para la capa Gold (Destino)
workspace_name = "RetailNova_Batch" # **¡CORREGIDO! Nombre del workspace correcto: RetailNova_workflow**

# Construir las rutas base ABFS para los Lakehouses Silver y Gold
silver_layer_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_name}.Lakehouse/Tables/"
gold_layer_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_name}.Lakehouse/Tables/"
gold_files_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_name}.Lakehouse/Files/" # Ruta para archivos (como high-watermark)


# Definir la ruta a la tabla de origen en Silver
silver_salesorderlines_path = silver_layer_abfs_base_path + "SalesOrderLines_Silver"

# Definir las rutas a las tablas de destino en Gold (Hechos y Dimensiones)
gold_salesfact_path = gold_layer_abfs_base_path + "FactSales"      # Tabla de Hechos
gold_dimcustomer_path = gold_layer_abfs_base_path + "DimCustomer"  # Tabla de Dimensión Cliente
gold_dimproduct_path = gold_layer_abfs_base_path + "DimProduct"    # Tabla de Dimensión Producto
gold_dimstore_path = gold_layer_abfs_base_path + "DimStore"      # Tabla de Dimensión Tienda (basada en ShippingCity)
gold_dimdate_path = gold_layer_abfs_base_path + "DimDate"        # Tabla de Dimensión Fecha
gold_dimcategory_path = gold_layer_abfs_base_path + "DimCategory" # Tabla de Dimensión Categoría


# Definir la ruta para el archivo de marca de agua (high-watermark) para Silver a Gold
# Lo guardaremos en la sección Files/HighWatermark dentro de LH_Gold.
high_watermark_gold_abfs_path = gold_files_abfs_base_path + "HighWatermark/silver_sales_high_watermark"


print("--- DEBUG: Rutas definidas (Silver a Gold) ---")
print(f"Silver SalesOrderLines (Origen): {silver_salesorderlines_path}")
print(f"Gold FactSales (Destino): {gold_salesfact_path}")
print(f"Gold DimCustomer (Destino): {gold_dimcustomer_path}")
print(f"Gold DimProduct (Destino): {gold_dimproduct_path}")
print(f"Gold DimStore (Destino): {gold_dimstore_path}")
print(f"Gold DimDate (Destino): {gold_dimdate_path}")
print(f"Gold DimCategory (Destino): {gold_dimcategory_path}")
print(f"High-Watermark File (Gold): {high_watermark_gold_abfs_path}")


# COMMAND ----------

# --- 1. Leer la marca de agua (high-watermark) para Silver a Gold ---
print("\n--- DEBUG: Intentando leer la marca de agua (Silver a Gold) ---")

try:
    # Intentar leer la marca de agua desde la ruta ABFS en LH_Gold (Files)
    df_high_watermark_gold = spark.read.format("delta").load(high_watermark_gold_abfs_path)
    last_processed_timestamp_gold = df_high_watermark_gold.select("LastProcessedDate").collect()[0][0]
    print(f"--- DEBUG: Marca de agua (Silver a Gold) leída: {last_processed_timestamp_gold} ---")
except Exception as e:
    # Si el archivo no existe o hay error al leer, inicializar la marca de agua a una fecha muy antigua
    print(f"--- DEBUG: No se pudo leer el archivo de marca de agua (Silver a Gold). Inicializando marca de agua. Error: {e} ---")
    last_processed_timestamp_gold = datetime.datetime.strptime("1900-01-01", '%Y-%m-%d')
    # Crear el archivo de marca de agua inicial en LH_Gold (Files)
    try:
        spark.createDataFrame([(last_processed_timestamp_gold,)], ["LastProcessedDate"]) \
             .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
             .write.format("delta").mode("overwrite").save(high_watermark_gold_abfs_path)
        print("--- DEBUG: Archivo de marca de agua inicial (Silver a Gold) creado en LH_Gold (Files). ---")
    except Exception as create_e:
        print(f"--- ERROR: Falló la creación del archivo de marca de agua inicial (Silver a Gold): {create_e} ---")
        # Log the error but continue with the initial date
        pass # Continue execution with initial date


print(f"--- DEBUG: Usando marca de agua (Silver a Gold): {last_processed_timestamp_gold} para filtrar datos de Silver. ---")


# COMMAND ----------

# --- 2. Leer datos Incrementales de Silver (Usando ruta ABFS) ---
print("\n--- DEBUG: Intentando leer datos de Silver desde la ruta ABFS: ---")
print(f"--- DEBUG: Leyendo de: {silver_salesorderlines_path} ---")

try:
    # Leer la tabla SalesOrderLines_Silver desde LH_Silver usando la ruta ABFS
    df_silver_data = spark.read.format("delta").load(silver_salesorderlines_path)
    print("--- DEBUG: Leído datos de Silver. ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de datos de Silver: {e} ---")
    sys.exit(1) # Exit if essential table cannot be read

print("\n--- DEBUG: Esquema de la tabla Silver ---")
df_silver_data.printSchema()

# Asegurarse de que ProcessingTimestamp en Silver es Timestamp para la comparación
df_silver_data = df_silver_data.withColumn("ProcessingTimestamp", col("ProcessingTimestamp").cast(TimestampType()))

# Filtrar los datos en Silver para obtener solo los nuevos desde la última marca de agua de Gold
# Usamos ProcessingTimestamp como el criterio incremental
df_new_silver_data = df_silver_data.filter(col("ProcessingTimestamp") > last_processed_timestamp_gold)


print(f"--- DEBUG: Conteo de nuevos registros en Silver después de filtrar por marca de agua: {df_new_silver_data.count()} ---")


# COMMAND ----------

# --- 3. Procesar a Gold si hay nuevos datos ---
print("\n--- DEBUG: Comprobando si hay nuevos datos para procesar a Gold ---")

if df_new_silver_data.count() == 0:
    print("No hay nuevos datos en Silver para procesar a Gold. El proceso Silver a Gold ha terminado.")
    # Si no hay nuevos datos, actualizar la marca de agua con la fecha actual
    # para evitar reprocesar los mismos datos en la próxima ejecución
    print("\nActualizando la marca de agua (Silver a Gold) con la fecha/hora actual ya que no se encontraron nuevos datos...")
    current_processing_timestamp_gold = datetime.datetime.now()
    try:
        spark.createDataFrame([(current_processing_timestamp_gold,)], ["LastProcessedDate"]) \
             .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
             .write.format("delta").mode("overwrite").save(high_watermark_gold_abfs_path)
        print(f"Marca de agua (Silver a Gold) actualizada a: {current_processing_timestamp_gold}")
    except Exception as update_e:
        print(f"--- ERROR: Falló la actualización de la marca de agua (Silver a Gold): {update_e} ---")
        # Log the error but continue to exit
        pass # Continue execution to exit

    print("Proceso Silver a Gold finalizado (sin nuevos datos).")
    sys.exit(0) # Termina la ejecución del notebook


print(f"Se encontraron {df_new_silver_data.count()} nuevos registros en Silver para procesar a Gold.")


# COMMAND ----------

# --- 4. Procesar y Cargar Tablas de Dimensión (SCD Type 1: Sobrescribir atributos) ---
print("\n--- DEBUG: Iniciando procesamiento y carga de Tablas de Dimensión ---")

# --- DimCustomer ---
print("--- DEBUG: Procesando DimCustomer ---")
# Seleccionar columnas relevantes para DimCustomer desde los nuevos datos de Silver
# Usamos CustomerSourceID como Business Key
df_new_customers = df_new_silver_data.select("CustomerSourceID", "CustomerName", "CustomerSegment").distinct().filter(col("CustomerSourceID").isNotNull()) # Asegurar BK no es nula

# Intentar leer la tabla DimCustomer existente en Gold
try:
    df_dimcustomer_existing = spark.read.format("delta").load(gold_dimcustomer_path)
    print("--- DEBUG: Leída tabla DimCustomer existente. ---")

    # Extraer solo las Business Keys existentes
    existing_customer_keys = df_dimcustomer_existing.select("CustomerSourceID").distinct()

    # Filtrar nuevos clientes que NO existen en la dimensión actual
    df_customers_to_add = df_new_customers.alias("new") \
        .join(existing_customer_keys.alias("existing"), on="CustomerSourceID", how="left_anti") \
        .select("new.*")

    print(f"--- DEBUG: Nuevos clientes a añadir a DimCustomer: {df_customers_to_add.count()} ---")

    if df_customers_to_add.count() > 0:
         # Generar clave subrogada (simple: usar monotonically_increasing_id + offset si la tabla no está vacía)
         # Una forma más robusta es usar una secuencia o hash, pero monotonically_increasing_id es simple para empezar.
         # Para APPEND, necesitamos asegurarnos de que las SK sean únicas globalmente.
         # Una forma es leer la SK máxima existente y sumar a los nuevos IDs.
         max_sk = df_dimcustomer_existing.select(max("CustomerKey")).collect()[0][0]
         sk_offset = max_sk + 1 if max_sk is not None else 1

         window_spec = Window.orderBy("CustomerSourceID") # Ordenar para IDs determinísticos dentro de este lote
         df_customers_to_add_with_sk = df_customers_to_add.withColumn("row_id", row_number().over(window_spec)) \
                                                        .withColumn("CustomerKey", col("row_id") + lit(sk_offset)) \
                                                        .select("CustomerKey", "CustomerSourceID", "CustomerName", "CustomerSegment")

         # Escribir/añadir los nuevos clientes a la tabla DimCustomer existente
         print(f"--- DEBUG: Intentando APPEND a DimCustomer en '{gold_dimcustomer_path}' ---")
         df_customers_to_add_with_sk.write.format("delta").mode("append").save(gold_dimcustomer_path)
         print(f"--- DEBUG: APPEND a DimCustomer completado. Añadidos {df_customers_to_add_with_sk.count()} nuevos clientes. ---")
    else:
         print("--- DEBUG: No hay nuevos clientes para añadir a DimCustomer. ---")


except Exception as e:
    # Si la tabla DimCustomer no existe, crearla con los nuevos datos
    print(f"--- DEBUG: La tabla DimCustomer no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---")
    # Generar clave subrogada inicial
    window_spec = Window.orderBy("CustomerSourceID")
    df_dimcustomer_new = df_new_customers.withColumn("row_id", row_number().over(window_spec)) \
                                         .withColumn("CustomerKey", col("row_id")) \
                                         .select("CustomerKey", "CustomerSourceID", "CustomerName", "CustomerSegment")

    # --- DEBUG: Añadido Try/Except específico para la escritura inicial ---
    try:
        print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimCustomer en '{gold_dimcustomer_path}' con {df_dimcustomer_new.count()} filas. ---")
        df_dimcustomer_new.write.format("delta").mode("overwrite").save(gold_dimcustomer_path) # Usar overwrite para la primera creación
        print(f"--- DEBUG: Tabla DimCustomer creada inicialmente con {df_dimcustomer_new.count()} registros. ---")
    except Exception as write_e:
        print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimCustomer: {write_e} ---")
        # Decide si quieres salir aquí o continuar. Continuar podría llevar a errores posteriores.
        # Por ahora, solo loggeamos y continuamos, pero esto podría ser un punto de salida.
        pass # Continuar a pesar del error de escritura inicial de DimCustomer


# --- DimProduct ---
print("\n--- DEBUG: Procesando DimProduct ---")
# Seleccionar columnas relevantes para DimProduct desde los nuevos datos de Silver
# Usamos ProductID_OLTP como Business Key
df_new_products = df_new_silver_data.select("ProductID_OLTP", "ProductName", "ProductCategory", "ProductBrand").distinct().filter(col("ProductID_OLTP").isNotNull()) # Asegurar BK no es nula

# Intentar leer la tabla DimProduct existente en Gold
try:
    df_dimproduct_existing = spark.read.format("delta").load(gold_dimproduct_path)
    print("--- DEBUG: Leída tabla DimProduct existente. ---")

    # Extraer solo las Business Keys existentes
    existing_product_keys = df_dimproduct_existing.select("ProductID_OLTP").distinct()

    # Filtrar nuevos productos que NO existen en la dimensión actual
    df_products_to_add = df_new_products.alias("new") \
        .join(existing_product_keys.alias("existing"), on="ProductID_OLTP", how="left_anti") \
        .select("new.*")

    print(f"--- DEBUG: Nuevos productos a añadir a DimProduct: {df_products_to_add.count()} ---")

    if df_products_to_add.count() > 0:
         # Generar clave subrogada (simple: usar monotonically_increasing_id + offset)
         max_sk = df_dimproduct_existing.select(max("ProductKey")).collect()[0][0]
         sk_offset = max_sk + 1 if max_sk is not None else 1

         window_spec = Window.orderBy("ProductID_OLTP")
         df_products_to_add_with_sk = df_products_to_add.withColumn("row_id", row_number().over(window_spec)) \
                                                        .withColumn("ProductKey", col("row_id") + lit(sk_offset)) \
                                                        .select("ProductKey", "ProductID_OLTP", "ProductName", "ProductCategory", "ProductBrand")

         # Escribir/añadir los nuevos productos a la tabla DimProduct existente
         print(f"--- DEBUG: Intentando APPEND a DimProduct en '{gold_dimproduct_path}' ---")
         df_products_to_add_with_sk.write.format("delta").mode("append").save(gold_dimproduct_path)
         print(f"--- DEBUG: APPEND a DimProduct completado. Añadidos {df_products_to_add_with_sk.count()} nuevos productos. ---")
    else:
         print("--- DEBUG: No hay nuevos productos para añadir a DimProduct. ---")

except Exception as e:
    # Si la tabla DimProduct no existe, crearla con los nuevos datos
    print(f"--- DEBUG: La tabla DimProduct no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---") # Added stacktrace for more info
    # Generar clave subrogada inicial
    window_spec = Window.orderBy("ProductID_OLTP")
    df_dimproduct_new = df_new_products.withColumn("row_id", row_number().over(window_spec)) \
                                       .withColumn("ProductKey", col("row_id")) \
                                       .select("ProductKey", "ProductID_OLTP", "ProductName", "ProductCategory", "ProductBrand")

    # --- DEBUG: Añadido Try/Except específico para la escritura inicial ---
    try:
        print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimProduct en '{gold_dimproduct_path}' con {df_dimproduct_new.count()} filas. ---")
        df_dimproduct_new.write.format("delta").mode("overwrite").save(gold_dimproduct_path) # Usar overwrite para la primera creación
        print(f"--- DEBUG: Tabla DimProduct creada inicialmente con {df_dimproduct_new.count()} registros. ---")
    except Exception as write_e:
        print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimProduct: {write_e} ---") # Added stacktrace
        pass # Continuar a pesar del error de escritura inicial de DimProduct


# --- DimStore (Basada en ShippingCity) ---
print("\n--- DEBUG: Procesando DimStore (basada en ShippingCity) ---")
# Seleccionar columnas relevantes para DimStore desde los nuevos datos de Silver
# Usamos ShippingCity como Business Key (limitación: asume que cada ciudad es una "tienda" única)
df_new_stores = df_new_silver_data.select("ShippingCity").distinct().filter(col("ShippingCity").isNotNull()) # Asegurar BK no es nula

# Intentar leer la tabla DimStore existente en Gold
try:
    df_dimstore_existing = spark.read.format("delta").load(gold_dimstore_path)
    print("--- DEBUG: Leída tabla DimStore existente. ---")

    # Extraer solo las Business Keys existentes
    existing_store_keys = df_dimstore_existing.select("CityName").distinct() # Usamos CityName como BK en la dimensión

    # Filtrar nuevas ciudades que NO existen en la dimensión actual
    df_stores_to_add = df_new_stores.alias("new") \
        .join(existing_store_keys.alias("existing"), on=col("new.ShippingCity") == col("existing.CityName"), how="left_anti") \
        .select(col("new.ShippingCity").alias("CityName")) # Renombrar a CityName para la dimensión

    print(f"--- DEBUG: Nuevas ciudades/tiendas a añadir a DimStore: {df_stores_to_add.count()} ---")

    if df_stores_to_add.count() > 0:
         # Generar clave subrogada (simple: usar monotonically_increasing_id + offset)
         max_sk = df_dimstore_existing.select(max("StoreKey")).collect()[0][0]
         sk_offset = max_sk + 1 if max_sk is not None else 1

         window_spec = Window.orderBy("CityName")
         df_stores_to_add_with_sk = df_stores_to_add.withColumn("row_id", row_number().over(window_spec)) \
                                                    .withColumn("StoreKey", col("row_id") + lit(sk_offset)) \
                                                    .select("StoreKey", "CityName") # Seleccionar la SK y el nombre de la ciudad

         # Escribir/añadir los nuevos ciudades/tiendas a la tabla DimStore existente
         print(f"--- DEBUG: Intentando APPEND a DimStore en '{gold_dimstore_path}' ---")
         df_stores_to_add_with_sk.write.format("delta").mode("append").save(gold_dimstore_path)
         print(f"--- DEBUG: APPEND a DimStore completado. Añadidas {df_stores_to_add_with_sk.count()} nuevas ciudades/tiendas. ---")
    else:
         print("--- DEBUG: No hay nuevas ciudades/tiendas para añadir a DimStore. ---")

except Exception as e:
    # Si la tabla DimStore no existe, crearla con los nuevos datos
    print(f"--- DEBUG: La tabla DimStore no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---") # Added stacktrace
    # Generar clave subrogada inicial
    window_spec = Window.orderBy("ShippingCity")
    df_dimstore_new = df_new_stores.withColumn("row_id", row_number().over(window_spec)) \
                                    .withColumn("StoreKey", col("row_id")) \
                                    .select(col("ShippingCity").alias("CityName"), col("StoreKey")) # Renombrar y seleccionar SK

    # --- DEBUG: Añadido Try/Except específico para la escritura inicial ---
    try:
        print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimStore en '{gold_dimstore_path}' con {df_dimstore_new.count()} filas. ---")
        df_dimstore_new.write.format("delta").mode("overwrite").save(gold_dimstore_path) # Usar overwrite para la primera creación
        print(f"--- DEBUG: Tabla DimStore creada inicialmente con {df_dimstore_new.count()} registros. ---")
    except Exception as write_e:
        print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimStore: {write_e} ---") # Added stacktrace
        pass # Continuar a pesar del error de escritura inicial de DimStore


# --- DimDate ---
print("\n--- DEBUG: Procesando DimDate ---")
# Extraer fechas únicas de los nuevos datos de Silver
df_new_dates = df_new_silver_data.select(col("OrderDate").cast("date").alias("OrderDate")).distinct().filter(col("OrderDate").isNotNull()) # Asegurar BK no es nula

# Intentar leer la tabla DimDate existente en Gold
try:
    df_dimdate_existing = spark.read.format("delta").load(gold_dimdate_path)
    print("--- DEBUG: Leída tabla DimDate existente. ---")

    # Extraer solo las fechas existentes
    existing_dates = df_dimdate_existing.select(col("Date").cast("date").alias("Date")).distinct() # Asumir que la columna de fecha en DimDate se llama 'Date'

    # Filtrar nuevas fechas que NO existen en la dimensión actual
    df_dates_to_add = df_new_dates.alias("new") \
        .join(existing_dates.alias("existing"), on=col("new.OrderDate") == col("existing.Date"), how="left_anti") \
        .select("new.OrderDate")

    print(f"--- DEBUG: Nuevas fechas a añadir a DimDate: {df_dates_to_add.count()} ---")

    if df_dates_to_add.count() > 0:
        # Generar atributos de fecha básicos para las nuevas fechas
        df_dates_to_add_with_attrs = df_dates_to_add.select(
            col("OrderDate").alias("Date"),
            date_format("OrderDate", "yyyyMMdd").cast(IntegerType()).alias("DateKey"), # Clave subrogada simple:YYYYMMDD
            date_format("OrderDate", "dd-MM-yyyy").alias("FullDate"), # <-- CORREGIDO: Formato dd-MM-yyyy
            date_format("OrderDate", "MM").alias("Month"),
            date_format("OrderDate", "MMMM").alias("MonthName"),
            date_format("OrderDate", "yyyy").alias("Year"),
            # --- CORRECCIÓN AQUÍ: Usar dayofweek y ajustar para 1=Monday ---
            # dayofweek() returns 1=Sunday, 7=Saturday. We want 1=Monday, 7=Sunday.
            # (dayofweek + 5) % 7 + 1 maps 1->7, 2->1, 3->2, ..., 7->6
            ((dayofweek(col("OrderDate")) + 5) % 7 + 1).alias("DayOfWeek"),
            # --- FIN CORRECCIÓN ---
            date_format("OrderDate", "EEEE").alias("DayOfWeekName"),
            date_format("OrderDate", "d").alias("DayOfMonth"),
            date_format("OrderDate", "D").alias("DayOfYear"),
            date_format("OrderDate", "q").alias("Quarter"),
            # --- CORRECCIÓN AQUÍ: Usar weekofyear function ---
            weekofyear(col("OrderDate")).alias("WeekOfYear") # Semana del año (ISO 8601)
            # --- FIN CORRECCIÓN ---
        )

        # Escribir/añadir los nuevos fechas a la tabla DimDate existente
        print(f"--- DEBUG: Intentando APPEND a DimDate en '{gold_dimdate_path}' ---")
        df_dates_to_add_with_attrs.write.format("delta").mode("append").save(gold_dimdate_path)
        print(f"--- DEBUG: APPEND a DimDate completado. Añadidas {df_dates_to_add_with_attrs.count()} nuevas fechas. ---")
    else:
         print("--- DEBUG: No hay nuevas fechas para añadir a DimDate. ---")

except Exception as e:
    # Si la tabla DimDate no existe, crearla con los nuevos datos
    print(f"--- DEBUG: La tabla DimDate no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---") # Added stacktrace
    # Generar atributos de fecha básicos para las nuevas fechas
    df_dimdate_new = df_new_dates.select(
        col("OrderDate").alias("Date"),
        date_format("OrderDate", "yyyyMMdd").cast(IntegerType()).alias("DateKey"), # Clave subrogada simple:YYYYMMDD
        date_format("OrderDate", "dd-MM-yyyy").alias("FullDate"), # <-- CORREGIDO: Formato dd-MM-yyyy
        date_format("OrderDate", "MM").alias("Month"),
        date_format("OrderDate", "MMMM").alias("MonthName"),
        date_format("OrderDate", "yyyy").alias("Year"),
        # --- CORRECCIÓN AQUÍ: Usar dayofweek y ajustar para 1=Monday ---
        # dayofweek() returns 1=Sunday, 7=Saturday. We want 1=Monday, 7=Sunday.
        # (dayofweek + 5) % 7 + 1 maps 1->7, 2->1, 3->2, ..., 7->6
        ((dayofweek(col("OrderDate")) + 5) % 7 + 1).alias("DayOfWeek"),
        # --- FIN CORRECCIÓN ---
        date_format("OrderDate", "EEEE").alias("DayOfWeekName"),
        date_format("OrderDate", "d").alias("DayOfMonth"),
        date_format("OrderDate", "D").alias("DayOfYear"),
        date_format("OrderDate", "q").alias("Quarter"),
        # --- CORRECCIÓN AQUÍ: Usar weekofyear function ---
        weekofyear(col("OrderDate")).alias("WeekOfYear") # Semana del año (ISO 8601)
        # --- FIN CORRECCIÓN ---
    )

    # --- DEBUG: Añadido Try/Except específico para la escritura inicial ---
    try:
        print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimDate en '{gold_dimdate_path}' con {df_dimdate_new.count()} filas. ---")
        df_dimdate_new.write.format("delta").mode("overwrite").save(gold_dimdate_path) # Usar overwrite para la primera creación
        print(f"--- DEBUG: Tabla DimDate creada inicialmente con {df_dimdate_new.count()} registros. ---")
    except Exception as write_e:
        print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimDate: {write_e} ---") # Added stacktrace
        pass # Continuar a pesar del error de escritura inicial de DimDate


# --- DimCategory ---
print("\n--- DEBUG: Procesando DimCategory ---")
# Seleccionar columnas relevantes para DimCategory desde los nuevos datos de Silver
# Usamos ProductCategory como Business Key
df_new_categories = df_new_silver_data.select("ProductCategory").distinct().filter(col("ProductCategory").isNotNull()) # Asegurar BK no es nula

# Leer la tabla DimCategory existente en Gold (si existe)
try:
    # Intentar leer la tabla existente
    delta_dim_category = DeltaTable.forPath(spark, gold_dimcategory_path)
    print("--- DEBUG: Tabla DimCategory existente encontrada en Gold. ---")
    df_dim_category_existing = delta_dim_category.toDF()
    print("--- DEBUG: Leído tabla DimCategory existente. ---")

    # Identificar nuevas categorías que no existen en la tabla DimCategory de Gold
    df_categories_to_insert = df_new_categories.alias("new") \
        .join(df_dim_category_existing.alias("existing"), on=(col("new.ProductCategory") == col("existing.CategoryName")), how="left_anti") \
        .select(col("new.ProductCategory").alias("CategoryName")) # Renombrar para el esquema de la dimensión

    print(f"--- DEBUG: Conteo de nuevas categorías a insertar: {df_categories_to_insert.count()} ---")

    if df_categories_to_insert.count() > 0:
         # Generar una clave subrogada (CategoryKey) para las nuevas categorías
         # Leer la SK máxima existente para empezar a numerar desde ahí
         max_key_row = df_dim_category_existing.select(max("CategoryKey")).collect()[0]
         start_key = max_key_row[0] + 1 if max_key_row[0] is not None else 1

         window_spec = Window.orderBy("CategoryName")
         df_categories_with_key = df_categories_to_insert.withColumn("row_id", row_number().over(window_spec)) \
                                                         .withColumn("CategoryKey", col("row_id") + lit(start_key) - 1) \
                                                         .select("CategoryKey", "CategoryName")

         # Escribir/Actualizar la tabla DimCategory en Gold usando MERGE
         print(f"\n--- DEBUG: Intentando MERGE en DimCategory en '{gold_dimcategory_path}'... ---")
         delta_dim_category.alias("target") \
             .merge(
                 df_categories_with_key.alias("source"),
                 "target.CategoryName = source.CategoryName" # Unir por el nombre de la categoría
             ) \
             .whenNotMatchedInsert(
                 values = {
                     "CategoryKey": col("source.CategoryKey"), # Insertar la nueva clave generada
                     "CategoryName": col("source.CategoryName")
                 }
             ) \
             .execute()
         print("--- DEBUG: MERGE en DimCategory ejecutado. ---")
    else:
        print("--- DEBUG: No hay nuevas categorías para insertar en DimCategory. ---")


except Exception as e:
    # Si la tabla Delta no existe, simplemente escribir el DataFrame inicial
    print(f"--- DEBUG: Tabla DimCategory no encontrada en Gold o error al leerla. Intentando CREARLA. Error: {e} ---") # Added stacktrace
    # Extraer categorías únicas de los nuevos datos de Silver (asegurando no nulas)
    df_new_categories_for_create = df_new_silver_data.select("ProductCategory").distinct().filter(col("ProductCategory").isNotNull())

    # Generar clave subrogada inicial
    window_spec = Window.orderBy("ProductCategory")
    df_dim_category_new = df_new_categories_for_create.withColumn("row_id", row_number().over(window_spec)) \
                                                      .withColumn("CategoryKey", col("row_id")) \
                                                      .select(col("ProductCategory").alias("CategoryName"), col("CategoryKey")) # Renombrar y seleccionar SK

    # --- DEBUG: Añadido Try/Except específico para la escritura inicial ---
    try:
        print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimCategory en '{gold_dimcategory_path}' con {df_dim_category_new.count()} filas. ---")
        df_dim_category_new.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_dimcategory_path) # Usar overwrite para la primera creación
        print(f"--- DEBUG: Tabla DimCategory creada inicialmente con {df_dim_category_new.count()} registros. ---")
    except Exception as write_e:
        print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimCategory: {write_e} ---") # Added stacktrace
        pass # Continuar a pesar del error de escritura inicial de DimCategory


print("--- DEBUG: Proceso de Tablas de Dimensión completado.")


# COMMAND ----------

# --- 5. Procesar y Cargar DimDate ---
# Este paso se movió arriba (paso 4) para mantener todas las dimensiones juntas.
# El código de DimDate ya está en el paso 4.


# COMMAND ----------

# --- 6. Unir datos incrementales de Silver con Tablas de Dimensión para obtener Claves Subrogadas ---
print("\n--- DEBUG: Uniendo datos de Silver con Dimensiones para obtener Claves Subrogadas ---")

# Leer las tablas de dimensión completas (incluyendo los registros recién añadidos)
# Usamos .load() después de la escritura/append para asegurarnos de leer la versión más reciente de la dimensión
try:
    df_dimcustomer = spark.read.format("delta").load(gold_dimcustomer_path)
    df_dimproduct = spark.read.format("delta").load(gold_dimproduct_path)
    df_dimstore = spark.read.format("delta").load(gold_dimstore_path)
    df_dimdate = spark.read.format("delta").load(gold_dimdate_path)
    df_dimcategory = spark.read.format("delta").load(gold_dimcategory_path) # Leer la nueva DimCategory
    print("--- DEBUG: Leídas tablas de dimensión actualizadas. ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de tablas de dimensión actualizadas: {e} ---") # Added stacktrace
    # Si las dimensiones no existen o fallan al leer, no podemos construir la tabla de hechos.
    # Es mejor salir aquí.
    print("--- ERROR: No se pudieron leer las tablas de dimensión. Saliendo. ---")
    sys.exit(1)


# Unir los nuevos datos de Silver con las dimensiones para obtener las claves subrogadas
# Usamos LEFT OUTER JOIN para incluir filas de hechos incluso si no tienen una coincidencia en la dimensión (aunque idealmente deberían tenerla)
# Si necesitas INNER JOIN (solo filas de hechos con correspondencia en TODAS las dimensiones), cambia how="inner"
df_factsales_new = df_new_silver_data.alias("silver") \
    .join(df_dimcustomer.alias("cust"), on=(col("silver.CustomerSourceID") == col("cust.CustomerSourceID")), how="left_outer") \
    .join(df_dimproduct.alias("prod"), on=(col("silver.ProductID_OLTP") == col("prod.ProductID_OLTP")), how="left_outer") \
    .join(df_dimstore.alias("store"), on=(col("silver.ShippingCity") == col("store.CityName")), how="left_outer") \
    .join(df_dimdate.alias("date"), on=(col("silver.OrderDate").cast("date") == col("date.Date")), how="left_outer") \
    .join(df_dimcategory.alias("cat"), on=(col("silver.ProductCategory") == col("cat.CategoryName")), how="left_outer") \
    .select(
        # Clave de Hecho (puede ser OrderItemID o una nueva SK para la línea de hecho si se necesita)
        col("silver.OrderItemKey").alias("SalesOrderLineKey"), # Usamos OrderItemKey como clave de la línea de venta por simplicidad

        # Claves Subrogadas de Dimensión
        # Usar coalesce para asignar una clave de "Desconocido" (-1) si la unión falla
        coalesce(col("cust.CustomerKey"), lit(-1)).alias("CustomerKey"),
        coalesce(col("prod.ProductKey"), lit(-1)).alias("ProductKey"),
        coalesce(col("store.StoreKey"), lit(-1)).alias("StoreKey"),
        coalesce(col("date.DateKey"), lit(-1)).alias("OrderDateKey"), # Usamos DateKey de DimDate
        coalesce(col("cat.CategoryKey"), lit(-1)).alias("CategoryKey"), # <-- ADDED CategoryKey with coalesce for -1

        # Atributos Directos o Business Keys (Opcional, si se necesitan en la tabla de hechos)
        col("silver.OrderSourceID").alias("OrderSourceID"), # Business Key de Orden
        col("silver.CustomerSourceID").alias("CustomerSourceID"), # Business Key de Cliente
        col("silver.ProductID_OLTP").alias("ProductID_OLTP"), # Business Key de Producto
        col("silver.ShippingCity").alias("ShippingCity"), # Business Key de Tienda (Ciudad)
        col("silver.PaymentMethod").alias("PaymentMethod"),
        col("silver.ShippingMethod").alias("ShippingMethod"),
        col("silver.OrderStatus").alias("OrderStatus"),
        col("silver.CustomerSegment").alias("CustomerSegment"), # Atributo de Cliente (considerar mover a DimCustomer si cambia)
        col("silver.DeviceType").alias("DeviceType"),
        col("silver.ReferralSource").alias("ReferralSource"),
        col("silver.PromotionApplied").alias("PromotionApplied"),

        # Métricas
        coalesce(col("silver.Quantity_Cleaned"), lit(0)).alias("Quantity"), # Ensure cleaned columns are used and coalesce again just in case
        coalesce(col("silver.PricePerUnit_Cleaned"), lit(0.0)).alias("PricePerUnit"), # Ensure cleaned columns are used and coalesce again just in case
        coalesce(col("silver.DiscountPct_Cleaned"), lit(0.0)).alias("DiscountPct"), # Ensure cleaned columns are used and coalesce again just in case
        coalesce(col("silver.SalesAmountLineItem"), lit(0.0)).alias("SalesAmount"), # Ensure calculated columns are used and coalesce
        coalesce(col("silver.DiscountAmountLineItem"), lit(0.0)).alias("DiscountAmount"), # Ensure calculated columns are used and coalesce
        coalesce(col("silver.GrossAmountLineItem"), lit(0.0)).alias("GrossAmount"), # Ensure calculated columns are used and coalesce

        # Timestamps
        col("silver.OrderDate").alias("OrderTimestamp"), # Timestamp original de la orden
        col("silver.ProcessingTimestamp").alias("ProcessingTimestampSilver"), # Timestamp de procesamiento en Silver
        current_timestamp().alias("ProcessingTimestampGold") # Timestamp de procesamiento en Gold
    )

print("\n--- DEBUG: Esquema del DataFrame Resultante (FactSales) ---")
df_factsales_new.printSchema()


# COMMAND ----------

# --- 7. Escribir datos incrementales en la Capa Gold (Tabla de Hechos FactSales) ---
print(f"\n--- DEBUG: Escribiendo datos incrementales en la tabla Delta '{gold_salesfact_path}' en el Lakehouse '{gold_lakehouse_name}' usando ruta ABFS... ---")

# Escribir el DataFrame incremental en la tabla Delta FactSales en Gold.
# Usaremos el modo 'append' para añadir los nuevos registros incrementales.
# Usamos mergeSchema para permitir la adición de nuevas columnas if needed in the future.
df_factsales_new.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(gold_salesfact_path) # Usa 'append' para añadir los nuevos datos incrementales

print(f"--- DEBUG: Datos incrementales escritos exitosamente en la tabla FactSales en Gold usando ruta ABFS en '{gold_salesfact_path}'. ---")


# COMMAND ----------

# --- 8. Actualizar la marca de agua (high-watermark) para Silver a Gold ---
# Actualizar la marca de agua (high-watermark) con la fecha máxima de ProcessingTimestamp de los datos que acabamos de procesar
# Esto se hace DESPUÉS de que los datos han sido procesados y escritos a Gold.
print("\n--- DEBUG: Actualizando la marca de agua (Silver a Gold) con la fecha máxima de procesamiento de los datos recién cargados... ---")

# Calcular la fecha máxima de ProcessingTimestamp de los datos que se acaban de mover a Gold
# Usamos df_new_silver_data porque contiene solo los registros que se procesaron en esta ejecución
new_max_processing_timestamp_gold_row = df_new_silver_data.select(max("ProcessingTimestamp")).collect()[0]

if new_max_processing_timestamp_gold_row is not None and new_max_processing_timestamp_gold_row[0] is not None:
    new_max_processing_timestamp_gold = new_max_processing_timestamp_gold_row[0]
    df_new_high_watermark_gold = spark.createDataFrame([(new_max_processing_timestamp_gold,)], ["LastProcessedDate"]) \
                                     .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType()))

    # Escribir/Actualizar la tabla de high watermark en LH_Gold (Files) usando ruta ABFS
    print(f"--- DEBUG: Intentando escribir High Watermark (Silver a Gold) en la ruta ABFS: {high_watermark_gold_abfs_path} ---")
    df_new_high_watermark_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(high_watermark_gold_abfs_path) # Sobrescribe la tabla con la nueva fecha

    print(f"--- DEBUG: Marca de agua (Silver a Gold) actualizada a: {new_max_processing_timestamp_gold} ---")

else:
     print("Advertencia: No se encontraron nuevas fechas máximas de procesamiento válidas en los datos procesados para actualizar el High Watermark (Silver a Gold).")

print("\n--- Proceso Silver a Gold COMPLETADO ---")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
