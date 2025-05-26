# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "22a342f7-f9db-43b7-9bdb-134402aa63ff",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "0933622e-1b13-4fad-98cf-cac982f928b9",
# META       "known_lakehouses": [
# META         {
# META           "id": "22a342f7-f9db-43b7-9bdb-134402aa63ff"
# META         },
# META         {
# META           "id": "0935ecad-505a-4d1a-8231-9715e56be5fd"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Transformation

# PARAMETERS CELL ********************

# COMMAND ----------
# Importar las librerías necesarias

import datetime
from pyspark.sql.functions import col, lit, coalesce, current_timestamp, date_format, max, row_number, monotonically_increasing_id, sha2, dayofweek, when, weekofyear
from pyspark.sql.types import TimestampType, IntegerType, DecimalType
from pyspark.sql.window import Window
# import sys # Ya no necesitamos sys si removemos sys.exit

# Importar mssparkutils para operaciones del sistema de archivos en Fabric
from notebookutils import mssparkutils

# Importar DeltaTable para operaciones MERGE
from delta.tables import DeltaTable

# COMMAND ----------

# Definir las rutas a las capas Silver y Gold en OneLake
silver_lakehouse_name = "" # Nombre del Lakehouse para la capa Silver (Origen)
gold_lakehouse_name = ""   # Nombre del Lakehouse para la capa Gold (Destino)
# **VERIFICADO DE LA SALIDA DEL NOTEBOOK ANTERIOR: RetailNova_Dev_Github**
workspace_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# Usamos una bandera para controlar la ejecución si falla la lectura inicial de Silver
should_continue = True

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
        print("--- ERROR: NO SE PUEDE CREAR ARCHIVO DE MARCA DE AGUA INICIAL. DETENIENDO PROCESO. ---")
        should_continue = False
        pass

if should_continue:
    print(f"--- DEBUG: Usando marca de agua (Silver a Gold): {last_processed_timestamp_gold} para filtrar datos de Silver. ---")


# COMMAND ----------

# Solo proceder si la lectura/inicialización del watermark fue exitosa
if should_continue:
    # --- 2. Leer datos Incrementales de Silver (Usando ruta ABFS) ---
    print("\n--- DEBUG: Intentando leer datos de Silver desde la ruta ABFS: ---")
    print(f"--- DEBUG: Leyendo de: {silver_salesorderlines_path} ---")

    try:
        # Leer la tabla SalesOrderLines_Silver desde LH_Silver usando la ruta ABFS
        df_silver_data = spark.read.format("delta").load(silver_salesorderlines_path)
        print("--- DEBUG: Leído datos de Silver. ---")
    except Exception as e:
        print(f"--- ERROR: Falló la lectura de datos de Silver: {e} ---")
        print("--- ERROR: NO SE PUEDEN LEER DATOS DE SILVER. DETENIENDO PROCESO. ---")
        should_continue = False

if should_continue:
    print("\n--- DEBUG: Esquema de la tabla Silver ---")
    df_silver_data.printSchema()

    # Asegurarse de que ProcessingTimestamp en Silver es Timestamp para la comparación
    df_silver_data = df_silver_data.withColumn("ProcessingTimestamp", col("ProcessingTimestamp").cast(TimestampType()))

    # Filtrar los datos en Silver para obtener solo los nuevos desde la última marca de agua de Gold
    # Usamos ProcessingTimestamp como el criterio incremental
    # Mantenemos ">" según tu lógica original. Si quisieras incluir datos con el mismo timestamp, usa ">=".
    df_new_silver_data = df_silver_data.filter(col("ProcessingTimestamp") > last_processed_timestamp_gold)


    print(f"--- DEBUG: Conteo de nuevos registros en Silver después de filtrar por marca de agua: {df_new_silver_data.count()} ---")

    # COMMAND ----------

    # Solo proceder si la lectura/inicialización del watermark y la lectura de Silver fueron exitosas
    if should_continue:
        # --- 3. Procesar a Gold si hay nuevos datos ---
        print("\n--- DEBUG: Comprobando si hay nuevos datos para procesar a Gold ---")

        if df_new_silver_data.count() == 0:
            print("No hay nuevos datos en Silver para procesar a Gold. El proceso Silver a Gold ha terminado.")
            print("\nActualizando la marca de agua (Silver a Gold) con la fecha/hora actual ya que no se encontraron nuevos datos...")
            current_processing_timestamp_gold = datetime.datetime.now()
            try:
                spark.createDataFrame([(current_processing_timestamp_gold,)], ["LastProcessedDate"]) \
                     .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
                     .write.format("delta").mode("overwrite").save(high_watermark_gold_abfs_path)
                print(f"Marca de agua (Silver a Gold) actualizada a: {current_processing_timestamp_gold}")
            except Exception as update_e:
                print(f"--- ERROR: Falló la actualización de la marca de agua (Silver a Gold): {update_e} ---")
                pass

            print("Proceso Silver a Gold finalizado (sin nuevos datos).")
            # La ejecución simplemente terminará la celda de forma limpia.

        else:
            print(f"Se encontraron {df_new_silver_data.count()} nuevos registros en Silver para procesar a Gold.")

            # COMMAND ----------

            # Esto solo se ejecuta si hay nuevos datos y should_continue es True
            # --- 4. Procesar y Cargar Tablas de Dimensión (SCD Type 1: Sobrescribir atributos) ---
            print("\n--- DEBUG: Iniciando procesamiento y carga de Tablas de Dimensión ---")

            # Usamos una bandera local para controlar si la carga de dimensiones fue exitosa antes de la tabla de hechos
            dimensions_load_successful = True

            # --- DimCustomer ---
            print("--- DEBUG: Procesando DimCustomer ---")
            df_new_customers = df_new_silver_data.select("CustomerSourceID", "CustomerName", "CustomerSegment").distinct().filter(col("CustomerSourceID").isNotNull())

            try:
                df_dimcustomer_existing = spark.read.format("delta").load(gold_dimcustomer_path)
                print("--- DEBUG: Leída tabla DimCustomer existente. ---")

                existing_customer_keys = df_dimcustomer_existing.select("CustomerSourceID").distinct()

                df_customers_to_add = df_new_customers.alias("new") \
                    .join(existing_customer_keys.alias("existing"), on="CustomerSourceID", how="left_anti") \
                    .select("new.*")

                print(f"--- DEBUG: Nuevos clientes a añadir a DimCustomer: {df_customers_to_add.count()} ---")

                if df_customers_to_add.count() > 0:
                     max_sk = df_dimcustomer_existing.select(max("CustomerKey")).collect()[0][0]
                     sk_offset = max_sk + 1 if max_sk is not None else 1

                     window_spec = Window.orderBy("CustomerSourceID")
                     df_customers_to_add_with_sk = df_customers_to_add.withColumn("row_id", row_number().over(window_spec)) \
                                                                    .withColumn("CustomerKey", col("row_id") + lit(sk_offset)) \
                                                                    .select("CustomerKey", "CustomerSourceID", "CustomerName", "CustomerSegment")

                     print(f"--- DEBUG: Intentando APPEND a DimCustomer en '{gold_dimcustomer_path}' ---")
                     df_customers_to_add_with_sk.write.format("delta").mode("append").save(gold_dimcustomer_path)
                     print(f"--- DEBUG: APPEND a DimCustomer completado. Añadidos {df_customers_to_add_with_sk.count()} nuevos clientes. ---")
                else:
                     print("--- DEBUG: No hay nuevos clientes para añadir a DimCustomer. ---")

            except Exception as e:
                print(f"--- DEBUG: La tabla DimCustomer no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---")
                window_spec = Window.orderBy("CustomerSourceID")
                df_dimcustomer_new = df_new_customers.withColumn("row_id", row_number().over(window_spec)) \
                                                     .withColumn("CustomerKey", col("row_id")) \
                                                     .select("CustomerKey", "CustomerSourceID", "CustomerName", "CustomerSegment")

                try:
                    print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimCustomer en '{gold_dimcustomer_path}' con {df_dimcustomer_new.count()} filas. ---")
                    df_dimcustomer_new.write.format("delta").mode("overwrite").save(gold_dimcustomer_path)
                    print(f"--- DEBUG: Tabla DimCustomer creada inicialmente con {df_dimcustomer_new.count()} registros. ---")
                except Exception as write_e:
                    print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimCustomer: {write_e} ---")
                    dimensions_load_successful = False # Considerar la creación inicial como crítica
                    pass


            # --- DimProduct ---
            print("\n--- DEBUG: Procesando DimProduct ---")
            df_new_products = df_new_silver_data.select("ProductID_OLTP", "ProductName", "ProductCategory", "ProductBrand").distinct().filter(col("ProductID_OLTP").isNotNull())

            try:
                df_dimproduct_existing = spark.read.format("delta").load(gold_dimproduct_path)
                print("--- DEBUG: Leída tabla DimProduct existente. ---")

                existing_product_keys = df_dimproduct_existing.select("ProductID_OLTP").distinct()

                df_products_to_add = df_new_products.alias("new") \
                    .join(existing_product_keys.alias("existing"), on="ProductID_OLTP", how="left_anti") \
                    .select("new.*")

                print(f"--- DEBUG: Nuevos productos a añadir a DimProduct: {df_products_to_add.count()} ---")

                if df_products_to_add.count() > 0:
                     max_sk = df_dimproduct_existing.select(max("ProductKey")).collect()[0][0]
                     sk_offset = max_sk + 1 if max_sk is not None else 1

                     window_spec = Window.orderBy("ProductID_OLTP")
                     df_products_to_add_with_sk = df_products_to_add.withColumn("row_id", row_number().over(window_spec)) \
                                                                    .withColumn("ProductKey", col("row_id") + lit(sk_offset)) \
                                                                    .select("ProductKey", "ProductID_OLTP", "ProductName", "ProductCategory", "ProductBrand")

                     print(f"--- DEBUG: Intentando APPEND a DimProduct en '{gold_dimproduct_path}' ---")
                     df_products_to_add_with_sk.write.format("delta").mode("append").save(gold_dimproduct_path)
                     print(f"--- DEBUG: APPEND a DimProduct completado. Añadidos {df_products_to_add_with_sk.count()} nuevos productos. ---")
                else:
                     print("--- DEBUG: No hay nuevos productos para añadir a DimProduct. ---")

            except Exception as e:
                print(f"--- DEBUG: La tabla DimProduct no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---")
                window_spec = Window.orderBy("ProductID_OLTP")
                df_dimproduct_new = df_new_products.withColumn("row_id", row_number().over(window_spec)) \
                                                   .withColumn("ProductKey", col("row_id")) \
                                                   .select("ProductKey", "ProductID_OLTP", "ProductName", "ProductCategory", "ProductBrand")

                try:
                    print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimProduct en '{gold_dimproduct_path}' con {df_dimproduct_new.count()} filas. ---")
                    df_dimproduct_new.write.format("delta").mode("overwrite").save(gold_dimproduct_path)
                    print(f"--- DEBUG: Tabla DimProduct creada inicialmente con {df_dimproduct_new.count()} registros. ---")
                except Exception as write_e:
                    print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimProduct: {write_e} ---")
                    dimensions_load_successful = False # Considerar la creación inicial como crítica
                    pass


            # --- DimStore (Basada en ShippingCity) ---
            print("\n--- DEBUG: Procesando DimStore (basada en ShippingCity) ---")
            df_new_stores = df_new_silver_data.select("ShippingCity").distinct().filter(col("ShippingCity").isNotNull())

            try:
                df_dimstore_existing = spark.read.format("delta").load(gold_dimstore_path)
                print("--- DEBUG: Leída tabla DimStore existente. ---")

                existing_store_keys = df_dimstore_existing.select("CityName").distinct()

                df_stores_to_add = df_new_stores.alias("new") \
                    .join(existing_store_keys.alias("existing"), on=col("new.ShippingCity") == col("existing.CityName"), how="left_anti") \
                    .select(col("new.ShippingCity").alias("CityName"))

                print(f"--- DEBUG: Nuevas ciudades/tiendas a añadir a DimStore: {df_stores_to_add.count()} ---")

                if df_stores_to_add.count() > 0:
                     max_sk = df_dimstore_existing.select(max("StoreKey")).collect()[0][0]
                     sk_offset = max_sk + 1 if max_sk is not None else 1

                     window_spec = Window.orderBy("CityName")
                     df_stores_to_add_with_sk = df_stores_to_add.withColumn("row_id", row_number().over(window_spec)) \
                                                                .withColumn("StoreKey", col("row_id") + lit(sk_offset)) \
                                                                .select("StoreKey", "CityName")

                     print(f"--- DEBUG: Intentando APPEND a DimStore en '{gold_dimstore_path}' ---")
                     df_stores_to_add_with_sk.write.format("delta").mode("append").save(gold_dimstore_path)
                     print(f"--- DEBUG: APPEND a DimStore completado. Añadidas {df_stores_to_add_with_sk.count()} nuevas ciudades/tiendas. ---")
                else:
                     print("--- DEBUG: No hay nuevas ciudades/tiendas para añadir a DimStore. ---")

            except Exception as e:
                print(f"--- DEBUG: La tabla DimStore no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---")
                window_spec = Window.orderBy("ShippingCity")
                df_dimstore_new = df_new_stores.withColumn("row_id", row_number().over(window_spec)) \
                                                .withColumn("StoreKey", col("row_id")) \
                                                .select(col("ShippingCity").alias("CityName"), col("StoreKey"))

                try:
                    print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimStore en '{gold_dimstore_path}' con {df_dimstore_new.count()} filas. ---")
                    df_dimstore_new.write.format("delta").mode("overwrite").save(gold_dimstore_path)
                    print(f"--- DEBUG: Tabla DimStore creada inicialmente con {df_dimstore_new.count()} registros. ---")
                except Exception as write_e:
                    print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimStore: {write_e} ---")
                    dimensions_load_successful = False # Considerar la creación inicial como crítica
                    pass


            # --- DimDate ---
            print("\n--- DEBUG: Procesando DimDate ---")
            df_new_dates = df_new_silver_data.select(col("OrderDate").cast("date").alias("OrderDate")).distinct().filter(col("OrderDate").isNotNull())

            try:
                df_dimdate_existing = spark.read.format("delta").load(gold_dimdate_path)
                print("--- DEBUG: Leída tabla DimDate existente. ---")

                existing_dates = df_dimdate_existing.select(col("Date").cast("date").alias("Date")).distinct()

                df_dates_to_add = df_new_dates.alias("new") \
                    .join(existing_dates.alias("existing"), on=col("new.OrderDate") == col("existing.Date"), how="left_anti") \
                    .select("new.OrderDate")

                print(f"--- DEBUG: Nuevas fechas a añadir a DimDate: {df_dates_to_add.count()} ---")

                if df_dates_to_add.count() > 0:
                    df_dates_to_add_with_attrs = df_dates_to_add.select(
                        col("OrderDate").alias("Date"),
                        date_format("OrderDate", "yyyyMMdd").cast(IntegerType()).alias("DateKey"),
                        date_format("OrderDate", "dd-MM-yyyy").alias("FullDate"),
                        date_format("OrderDate", "MM").alias("Month"),
                        date_format("OrderDate", "MMMM").alias("MonthName"),
                        date_format("OrderDate", "yyyy").alias("Year"),
                        # Spark dayofweek() devuelve 1=Domingo, 7=Sábado. Alias a DayOfWeek según tu lista.
                        dayofweek(col("OrderDate")).alias("DayOfWeek"), # <-- AJUSTADO NOMBRE
                        date_format("OrderDate", "EEEE").alias("DayOfWeekName"),
                        date_format("OrderDate", "d").alias("DayOfMonth"),
                        date_format("OrderDate", "D").alias("DayOfYear"),
                        date_format("OrderDate", "q").alias("Quarter"),
                        weekofyear(col("OrderDate")).alias("WeekOfYear")
                    )

                    print(f"--- DEBUG: Intentando APPEND a DimDate en '{gold_dimdate_path}' ---")
                    df_dates_to_add_with_attrs.write.format("delta").mode("append").save(gold_dimdate_path)
                    print(f"--- DEBUG: APPEND a DimDate completado. Añadidas {df_dates_to_add_with_attrs.count()} nuevas fechas. ---")
                else:
                     print("--- DEBUG: No hay nuevas fechas para añadir a DimDate. ---")

            except Exception as e:
                print(f"--- DEBUG: La tabla DimDate no existe o falló la lectura inicial. Intentando CREARLA. Error: {e} ---")
                df_dimdate_new = df_new_dates.select(
                    col("OrderDate").alias("Date"),
                    date_format("OrderDate", "yyyyMMdd").cast(IntegerType()).alias("DateKey"),
                    date_format("OrderDate", "dd-MM-yyyy").alias("FullDate"),
                    date_format("OrderDate", "MM").alias("Month"),
                    date_format("OrderDate", "MMMM").alias("MonthName"),
                    date_format("OrderDate", "yyyy").alias("Year"),
                    # Spark dayofweek() devuelve 1=Domingo, 7=Sábado. Alias a DayOfWeek según tu lista.
                    dayofweek(col("OrderDate")).alias("DayOfWeek"), # <-- AJUSTADO NOMBRE
                    date_format("OrderDate", "EEEE").alias("DayOfWeekName"),
                    date_format("OrderDate", "d").alias("DayOfMonth"),
                    date_format("OrderDate", "D").alias("DayOfYear"),
                    date_format("OrderDate", "q").alias("Quarter"),
                    weekofyear(col("OrderDate")).alias("WeekOfYear")
                )

                try:
                    print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimDate en '{gold_dimdate_path}' con {df_dimdate_new.count()} filas. ---")
                    df_dimdate_new.write.format("delta").mode("overwrite").save(gold_dimdate_path)
                    print(f"--- DEBUG: Tabla DimDate creada inicialmente con {df_dimdate_new.count()} registros. ---")
                except Exception as write_e:
                    print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimDate: {write_e} ---")
                    dimensions_load_successful = False # Considerar la creación inicial como crítica
                    pass


            # --- DimCategory ---
            print("\n--- DEBUG: Procesando DimCategory ---")
            df_new_categories = df_new_silver_data.select("ProductCategory").distinct().filter(col("ProductCategory").isNotNull())

            try:
                delta_dim_category = DeltaTable.forPath(spark, gold_dimcategory_path)
                print("--- DEBUG: Tabla DimCategory existente encontrada en Gold. ---")
                df_dim_category_existing = delta_dim_category.toDF()
                print("--- DEBUG: Leído tabla DimCategory existente. ---")

                df_categories_to_insert = df_new_categories.alias("new") \
                    .join(df_dim_category_existing.alias("existing"), on=(col("new.ProductCategory") == col("existing.CategoryName")), how="left_anti") \
                    .select(col("new.ProductCategory").alias("CategoryName"))

                print(f"--- DEBUG: Conteo de nuevas categorías a insertar: {df_categories_to_insert.count()} ---")

                if df_categories_to_insert.count() > 0:
                     max_key_row = df_dim_category_existing.select(max("CategoryKey")).collect()[0]
                     start_key = max_key_row[0] + 1 if max_key_row[0] is not None else 1

                     window_spec = Window.orderBy("CategoryName")
                     df_categories_with_key = df_categories_to_insert.withColumn("row_id", row_number().over(window_spec)) \
                                                                     .withColumn("CategoryKey", col("row_id") + lit(start_key) - 1) \
                                                                     .select("CategoryKey", "CategoryName")

                     print(f"\n--- DEBUG: Intentando MERGE en DimCategory en '{gold_dimcategory_path}'... ---")
                     delta_dim_category.alias("target") \
                         .merge(
                             df_categories_with_key.alias("source"),
                             "target.CategoryName = source.CategoryName"
                         ) \
                         .whenNotMatchedInsert(
                             values = {
                                 "CategoryKey": col("source.CategoryKey"),
                                 "CategoryName": col("source.CategoryName")
                             }
                         ) \
                         .execute()
                     print("--- DEBUG: MERGE en DimCategory ejecutado. ---")
                else:
                    print("--- DEBUG: No hay nuevas categorías para insertar en DimCategory. ---")


            except Exception as e:
                print(f"--- DEBUG: Tabla DimCategory no encontrada en Gold o error al leerla. Intentando CREARLA. Error: {e} ---")
                df_new_categories_for_create = df_new_silver_data.select("ProductCategory").distinct().filter(col("ProductCategory").isNotNull())

                window_spec = Window.orderBy("ProductCategory")
                df_dim_category_new = df_new_categories_for_create.withColumn("row_id", row_number().over(window_spec)) \
                                                                  .withColumn("CategoryKey", col("row_id")) \
                                                                  .select(col("ProductCategory").alias("CategoryName"), col("CategoryKey"))

                try:
                    print(f"--- DEBUG: Intentando OVERWRITE (Creación Inicial) de DimCategory en '{gold_dimcategory_path}' con {df_dim_category_new.count()} filas. ---")
                    df_dim_category_new.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .save(gold_dimcategory_path)
                    print(f"--- DEBUG: Tabla DimCategory creada inicialmente con {df_dim_category_new.count()} registros. ---")
                except Exception as write_e:
                    print(f"--- ERROR: Falló la CREACIÓN INICIAL (OVERWRITE) de la tabla DimCategory: {write_e} ---")
                    dimensions_load_successful = False # Considerar la creación inicial como crítica
                    pass


            print("--- DEBUG: Proceso de Tablas de Dimensión completado.")

            # COMMAND ----------

            # Esto solo se ejecuta si hay nuevos datos y la carga de dimensiones (asumimos) fue exitosa
            if dimensions_load_successful:
                # --- 5. Unir datos incrementales de Silver con Tablas de Dimensión para obtener Claves Subrogadas ---
                print("\n--- DEBUG: Uniendo datos de Silver con Dimensiones para obtener Claves Subrogadas ---")

                # Leer las tablas de dimensión completas (incluyendo los registros recién añadidos)
                try:
                    df_dimcustomer = spark.read.format("delta").load(gold_dimcustomer_path)
                    df_dimproduct = spark.read.format("delta").load(gold_dimproduct_path)
                    df_dimstore = spark.read.format("delta").load(gold_dimstore_path)
                    df_dimdate = spark.read.format("delta").load(gold_dimdate_path)
                    df_dimcategory = spark.read.format("delta").load(gold_dimcategory_path)
                    print("--- DEBUG: Leídas tablas de dimensión actualizadas. ---")
                except Exception as e:
                    print(f"--- ERROR: Falló la lectura de tablas de dimensión actualizadas: {e} ---")
                    print("--- ERROR: NO SE PUDIERON LEER LAS TABLAS DE DIMENSIÓN ACTUALIZADAS. DETENIENDO PROCESO DE HECHOS. ---")
                    dimensions_load_successful = False
                    pass


            # Solo proceder a construir y escribir la tabla de hechos si las dimensiones se cargaron/crearon exitosamente
            if dimensions_load_successful:
                # Debug: Verificar los datos antes de la transformación
                print("\n--- DEBUG: Valores ORIGINALES de descuento en Silver ---")
                df_new_silver_data.select(
                    "DiscountAmountLineItem",
                    col("DiscountAmountLineItem").cast("float").alias("AsFloat")
                ).summary().show()

                # Unir los nuevos datos de Silver con las dimensiones para obtener las claves subrogadas
                df_factsales_new = df_new_silver_data.alias("silver") \
                    .join(df_dimcustomer.alias("cust"), on=(col("silver.CustomerSourceID") == col("cust.CustomerSourceID")), how="left_outer") \
                    .join(df_dimproduct.alias("prod"), on=(col("silver.ProductID_OLTP") == col("prod.ProductID_OLTP")), how="left_outer") \
                    .join(df_dimstore.alias("store"), on=(col("silver.ShippingCity") == col("store.CityName")), how="left_outer") \
                    .join(df_dimdate.alias("date"), on=(col("silver.OrderDate").cast("date") == col("date.Date")), how="left_outer") \
                    .join(df_dimcategory.alias("cat"), on=(col("silver.ProductCategory") == col("cat.CategoryName")), how="left_outer") \
                    .select(
                        # Clave de Hecho
                        col("silver.OrderItemKey").alias("SalesOrderLineKey"),

                        # Claves Subrogadas de Dimensión
                        coalesce(col("cust.CustomerKey"), lit(-1)).alias("CustomerKey"),
                        coalesce(col("prod.ProductKey"), lit(-1)).alias("ProductKey"),
                        coalesce(col("store.StoreKey"), lit(-1)).alias("StoreKey"),
                        coalesce(col("date.DateKey"), lit(-1)).alias("OrderDateKey"),
                        coalesce(col("cat.CategoryKey"), lit(-1)).alias("CategoryKey"),

                        # Atributos Directos o Degenerados
                        col("silver.OrderSourceID").alias("OrderSourceID"),
                        col("silver.CustomerSourceID").alias("CustomerSourceID"),
                        col("silver.ProductID_OLTP").alias("ProductID_OLTP"),
                        col("silver.ShippingCity").alias("ShippingCity"),
                        col("silver.PaymentMethod").alias("PaymentMethod"),
                        col("silver.ShippingMethod").alias("ShippingMethod"), 
                        col("silver.OrderStatus").alias("OrderStatus"), 
                        col("silver.CustomerSegment").alias("CustomerSegment"), 
                        col("silver.DeviceType").alias("DeviceType"), 
                        col("silver.ReferralSource").alias("ReferralSource"), 
                        col("silver.PromotionApplied").alias("PromotionApplied"), 

                        # Medidas (Usar nombres limpios de Silver)
                        col("silver.Quantity").alias("Quantity"),
                        col("silver.PricePerUnit").cast("float").alias("PricePerUnit"),
                        col("silver.DiscountPct").cast("float").alias("DiscountPct"),
                        col("silver.SalesAmountLineItem").cast("float").alias("SalesAmount"), 

                        # CORRECCIÓN PARA DISCOUNTAMOUNT:
                        when(col("silver.DiscountAmountLineItem").isNull(), 0.0)
                            .otherwise(col("silver.DiscountAmountLineItem"))
                            .cast("float")
                            .alias("DiscountAmount"),
            
                        col("silver.GrossAmountLineItem").cast("float").alias("GrossAmount"),

                        # Timestamps
                        col("silver.OrderDate").alias("OrderTimestamp"), 
                        col("silver.ProcessingTimestamp").alias("ProcessingTimestampSilver"), 
                        current_timestamp().alias("ProcessingTimestampGold") 

                    )
                    
                # Debug: Verificar los resultados
                print("\n--- DEBUG: Valores de DiscountAmount después de transformación ---")
                df_factsales_new.select(
                    "DiscountAmount",
                    (col("SalesAmount") * col("DiscountPct")).alias("CalculatedDiscount")
                ).summary().show()

                print("\n--- DEBUG: Esquema del DataFrame Resultante (Tabla de Hechos) ---")
                df_factsales_new.printSchema()

                # COMMAND ----------

                # Esto solo se ejecuta si hay nuevos datos y la carga de dimensiones y la unión fueron exitosas
                # --- 6. Escribir datos a la capa Gold (FactSales) ---
                print("\n--- DEBUG: Escribiendo datos a la tabla FactSales ---")

                try:
                    # Usar modo 'append' para añadir nuevos datos a la tabla FactSales existente
                    # Considera usar .option("mergeSchema", "true") aquí también si esperas cambios en el esquema de Silver
                    df_factsales_new.write.format("delta").mode("append").save(gold_salesfact_path)
                    print("--- DEBUG: Datos escritos a la tabla FactSales. ---")

                    # COMMAND ----------

                    # Esto solo se ejecuta si la escritura a FactSales fue exitosa
                    # --- 7. Actualizar la marca de agua (Silver a Gold) ---
                    print("\n--- DEBUG: Actualizando la marca de agua (Silver a Gold) con el máximo ProcessingTimestamp de los datos procesados... ---")

                    new_high_watermark_gold = df_new_silver_data.agg(max("ProcessingTimestamp")).collect()[0][0]

                    if new_high_watermark_gold is None:
                         new_high_watermark_gold = datetime.datetime.now()
                         print("--- ADVERTENCIA: No se pudo determinar la marca de agua a partir de los datos procesados. Usando la marca de tiempo actual. ---")


                    try:
                        spark.createDataFrame([(new_high_watermark_gold,)], ["LastProcessedDate"]) \
                             .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
                             .write.format("delta").mode("overwrite").save(high_watermark_gold_abfs_path)
                        print(f"--- DEBUG: Marca de agua (Silver a Gold) actualizada a: {new_high_watermark_gold} ---")
                    except Exception as update_e:
                        print(f"--- ERROR: Falló la actualización final de la marca de agua (Silver a Gold): {update_e} ---")
                        pass

                    print("\nProceso Silver a Gold completado exitosamente.")

                except Exception as write_e:
                    print(f"--- ERROR: Falló la escritura a la tabla FactSales: {write_e} ---")
                    should_continue = False # Si falla la escritura de hechos, detener cualquier paso posterior.
                    pass

            else:
                print("\n--- INFO: Se omitió la construcción y escritura de la tabla de hechos debido a la falta de datos nuevos o errores en la carga de dimensiones. ---")


# Este else se ejecuta si should_continue se volvió False en alguna etapa inicial (lectura watermark o lectura Silver)
else:
    print("\nProceso Silver a Gold finalizado debido a errores en la lectura del watermark o datos de Silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
