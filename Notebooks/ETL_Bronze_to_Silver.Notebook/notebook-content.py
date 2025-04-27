# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8b1b017-3b83-4294-a9ea-7fc47416e097",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "703af36c-d0b8-4993-a4dd-4a53f1164897",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8b1b017-3b83-4294-a9ea-7fc47416e097"
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
from pyspark.sql.functions import col, lit, coalesce, current_timestamp, date_format
from pyspark.sql.types import TimestampType
import sys

# Importar mssparkutils para operaciones del sistema de archivos en Fabric
from notebookutils import mssparkutils

# COMMAND ----------

# Definir las rutas a las capas Bronze y Silver en OneLake
# ¡CORREGIDO! Las tablas de la capa Bronze están en LH_Bronze.
# ¡CORREGIDO! La tabla de la capa Silver está en LH_Silver.
# Asegúrate de que estos nombres de Lakehouse y la ruta ABFS sean correctos para tu entorno.
bronze_lakehouse_name = "LH_Bronze" # Nombre del Lakehouse para la capa Bronze
silver_lakehouse_name = "LH_Silver" # Nombre del Lakehouse para la capa Silver
workspace_name = "RetailNova_Batchv2" # **AJUSTA ESTO A TU NOMBRE DE WORKSPACE REAL**

# Construir las rutas base ABFS para los Lakehouses Bronze y Silver
bronze_layer_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_name}.Lakehouse/Tables/"
silver_layer_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_name}.Lakehouse/Tables/"


# Definir las rutas a las tablas específicas en Bronze (¡Ahora apuntando a LH_Bronze!)
bronze_orders_path = bronze_layer_abfs_base_path + "Orders"
bronze_orderitems_path = bronze_layer_abfs_base_path + "OrderItems"
bronze_customers_path = bronze_layer_abfs_base_path + "Customers"
bronze_products_path = bronze_layer_abfs_base_path + "Products"

# Definir la ruta a la tabla de destino en Silver (¡Ahora apuntando a LH_Silver!)
silver_salesorderlines_path = silver_layer_abfs_base_path + "SalesOrderLines_Silver"

# Definir la ruta para el archivo de marca de agua (high-watermark)
# ¡CORREGIDO! El archivo de marca de agua también debe estar en el Lakehouse Bronze (LH_Bronze)
# para que esté cerca de los datos de origen que controla.
# Lo guardaremos en la sección Files/HighWatermark dentro de LH_Bronze.
high_watermark_abfs_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_name}.Lakehouse/Files/HighWatermark/bronze_orders_high_watermark"


print("--- DEBUG: Rutas definidas ---")
print(f"Bronze Orders: {bronze_orders_path}")
print(f"Bronze OrderItems: {bronze_orderitems_path}")
print(f"Bronze Customers: {bronze_customers_path}")
print(f"Bronze Products: {bronze_products_path}")
print(f"Silver SalesOrderLines: {silver_salesorderlines_path}")
print(f"High-Watermark File: {high_watermark_abfs_path}")


# COMMAND ----------

# --- 1. Leer datos de la capa Bronze ---
print("\n--- DEBUG: Intentando leer datos de Bronze ---")

try:
    df_orders_bronze = spark.read.format("delta").load(bronze_orders_path)
    print("--- DEBUG: Leído Bronze Orders ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de Bronze Orders: {e} ---")
    sys.exit(1) # Exit if essential table cannot be read

try:
    df_orderitems_bronze = spark.read.format("delta").load(bronze_orderitems_path)
    print("--- DEBUG: Leído Bronze OrderItems ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de Bronze OrderItems: {e} ---")
    sys.exit(1) # Exit if essential table cannot be read

try:
    df_customers_bronze = spark.read.format("delta").load(bronze_customers_path)
    print("--- DEBUG: Leído Bronze Customers ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de Bronze Customers: {e} ---")
    # Customers might not be essential for filtering, but needed for join.
    # Decide if you want to exit or handle missing customers. Let's exit for now.
    sys.exit(1)

try:
    df_products_bronze = spark.read.format("delta").load(bronze_products_path)
    print("--- DEBUG: Leído Bronze Products ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de Bronze Products: {e} ---")
    # Products might not be essential for filtering, but needed for join.
    # Decide if you want to exit or handle missing products. Let's exit for now.
    sys.exit(1)

print("\n--- DEBUG: Esquemas de tablas Bronze ---")
df_orders_bronze.printSchema()
df_orderitems_bronze.printSchema()
df_customers_bronze.printSchema()
df_products_bronze.printSchema()


# COMMAND ----------

# --- 2. Leer la marca de agua (high-watermark) ---
print("\n--- DEBUG: Intentando leer la marca de agua (high-watermark) ---")

try:
    df_high_watermark = spark.read.format("delta").load(high_watermark_abfs_path)
    last_processed_date = df_high_watermark.select("LastProcessedDate").collect()[0][0]
    print(f"--- DEBUG: Marca de agua (high-watermark) leída: {last_processed_date} ---")
except Exception as e:
    # Si el archivo no existe o hay error al leer, inicializar la marca de agua a una fecha muy antigua
    print(f"--- DEBUG: No se pudo leer el archivo de marca de agua. Inicializando marca de agua. Error: {e} ---")
    last_processed_date = datetime.datetime.strptime("1900-01-01", '%Y-%m-%d')
    # Crear el archivo de marca de agua inicial con la fecha antigua
    try:
        spark.createDataFrame([(last_processed_date,)], ["LastProcessedDate"]) \
             .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
             .write.format("delta").mode("overwrite").save(high_watermark_abfs_path)
        print("--- DEBUG: Archivo de marca de agua inicial creado. ---")
    except Exception as create_e:
        print(f"--- ERROR: Falló la creación del archivo de marca de agua inicial: {create_e} ---")
        # Log the error but continue with the initial date
        pass # Continue execution with initial date


print(f"--- DEBUG: Usando marca de agua: {last_processed_date} para filtrar datos de Bronze. ---")


# COMMAND ----------

# --- 3. Filtrar datos nuevos usando la marca de agua ---
print("\n--- DEBUG: Filtrando datos nuevos de Bronze ---")

# Asegurarse de que OrderDate en Bronze es Timestamp para la comparación
# Si inferSchema ya lo hizo bien, esto no es estrictamente necesario, pero añade robustez
df_orders_bronze = df_orders_bronze.withColumn("OrderDate", col("OrderDate").cast(TimestampType()))

# Filtrar las órdenes en Bronze para obtener solo las nuevas desde la última marca de agua
df_new_orders = df_orders_bronze.alias("o").filter(col("o.OrderDate") > last_processed_date)

# Obtener los OrderID de las nuevas órdenes
new_order_ids = df_new_orders.select("OrderID").distinct().rdd.flatMap(lambda x: x).collect()

# Filtrar OrderItems para obtener solo los ítems relacionados con las nuevas órdenes
df_new_orderitems = df_orderitems_bronze.alias("oi").filter(col("oi.OrderID").isin(new_order_ids))

print(f"--- DEBUG: Conteo de nuevas órdenes después de filtrar por marca de agua: {df_new_orders.count()} ---")
print(f"--- DEBUG: Conteo de nuevos ítems de orden después de filtrar por OrderID: {df_new_orderitems.count()} ---")


# COMMAND ----------

# --- 4. Procesar a Silver si hay nuevos datos ---
print("\n--- DEBUG: Comprobando si hay nuevos datos para procesar a Silver ---")

if df_new_orders.count() == 0:
    print("No hay nuevas órdenes para procesar en Bronze desde la última carga.")
    # Si no hay nuevos datos, actualizar la marca de agua con la fecha actual
    # para evitar reprocesar los mismos datos en la próxima ejecución
    print("\nActualizando la marca de agua (high-watermark) con la fecha/hora actual ya que no se encontraron nuevos datos...")
    current_processing_timestamp = datetime.datetime.now()
    try:
        spark.createDataFrame([(current_processing_timestamp,)], ["LastProcessedDate"]) \
             .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
             .write.format("delta").mode("overwrite").save(high_watermark_abfs_path)
        print(f"Marca de agua actualizada a: {current_processing_timestamp}")
    except Exception as update_e:
        print(f"--- ERROR: Falló la actualización de la marca de agua: {update_e} ---")
        # Log the error but continue to exit
        pass # Continue execution to exit

    print("Proceso Bronze a Silver finalizado (sin nuevos datos).")
    sys.exit(0) # Termina la ejecución del notebook


print(f"Se encontraron {df_new_orders.count()} nuevas órdenes para procesar.")


# COMMAND ----------

# --- 5. Unir, Limpiar y Transformar (De Bronze a Silver) ---
print("\n--- DEBUG: Iniciando transformación a Silver ---")

df_silver = df_new_orderitems.alias("oi") \
    .join(df_new_orders.alias("o"), on=(col("oi.OrderID") == col("o.OrderID")), how="inner") \
    .join(df_customers_bronze.alias("c"), on=(col("o.CustomerID") == col("c.CustomerID")), how="inner") \
    .join(df_products_bronze.alias("p"), on=(col("oi.ProductID") == col("p.ProductID")), how="inner") \
    .select(
        col("oi.OrderItemID").alias("OrderItemKey"),
        col("o.OrderSourceID").alias("OrderSourceID"),
        col("o.OrderDate").alias("OrderDate"),
        col("o.SessionID").alias("SessionID"),
        col("o.DeviceType").alias("DeviceType"),
        col("o.ReferralSource").alias("ReferralSource"),
        col("o.ShippingMethod").alias("ShippingMethod"),
        col("o.OrderStatus").alias("OrderStatus"),
        col("o.ShippingCity").alias("ShippingCity"),
        col("o.PaymentMethod").alias("PaymentMethod"),
        col("o.TransactionID").alias("TransactionID"),
        col("c.CustomerSourceID").alias("CustomerSourceID"),
        col("c.CustomerName").alias("CustomerName"),
        col("c.CustomerSegment").alias("CustomerSegment"), # Ensure this column exists in Bronze Customers
        col("p.ProductID").alias("ProductID_OLTP"),
        col("p.ProductName").alias("ProductName"),
        col("p.Category").alias("ProductCategory"),
        col("p.Brand").alias("ProductBrand"),
        col("oi.Quantity").alias("Quantity"),
        col("oi.PricePerUnit").alias("PricePerUnit"),
        col("oi.DiscountPct").alias("DiscountPct"),
        col("oi.PromotionApplied").alias("PromotionApplied"), # Ensure this column exists in Bronze OrderItems

        # Limpieza y cálculos
        coalesce(col("oi.Quantity"), lit(0)).cast("int").alias("Quantity_Cleaned"),
        coalesce(col("oi.PricePerUnit"), lit(0)).cast("decimal(18,2)").alias("PricePerUnit_Cleaned"),
        coalesce(col("oi.DiscountPct"), lit(0)).cast("decimal(5,2)").alias("DiscountPct_Cleaned"),

        (
            col("Quantity_Cleaned").cast("decimal(18,2)") *
            col("PricePerUnit_Cleaned").cast("decimal(18,2)") *
            (lit(1) - col("DiscountPct_Cleaned")/100).cast("decimal(18,2)")
        ).alias("SalesAmountLineItem"),

        (
            col("Quantity_Cleaned").cast("decimal(18,2)") *
            col("PricePerUnit_Cleaned").cast("decimal(18,2)") *
            (col("DiscountPct_Cleaned")/100).cast("decimal(18,2)")
        ).alias("DiscountAmountLineItem"),

        (
            col("Quantity_Cleaned").cast("decimal(18,2)") *
            col("PricePerUnit_Cleaned").cast("decimal(18,2)")
        ).alias("GrossAmountLineItem"),

        current_timestamp().alias("ProcessingTimestamp")
    )

print("\n--- DEBUG: Esquema del DataFrame Resultante (Capa Silver) ---")
df_silver.printSchema()


# COMMAND ----------

# --- 6. Escribir datos a la capa Silver (Delta Lake) ---
print("\n--- DEBUG: Escribiendo datos a la capa Silver ---")

# Usar modo 'append' para añadir nuevos datos a la tabla Silver existente
df_silver.write.format("delta").mode("append").save(silver_salesorderlines_path)

print("--- DEBUG: Datos escritos a la capa Silver. ---")


# COMMAND ----------

# --- 7. Actualizar la marca de agua (high-watermark) ---
# Actualizar la marca de agua (high-watermark) con la fecha de procesamiento actual
# Esto se hace DESPUÉS de que los datos han sido procesados y escritos a Silver.
print("\n--- DEBUG: Actualizando la marca de agua (high-watermark) con la fecha/hora actual... ---")
current_processing_timestamp = datetime.datetime.now()

try:
    spark.createDataFrame([(current_processing_timestamp,)], ["LastProcessedDate"]) \
         .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
         .write.format("delta").mode("overwrite").save(high_watermark_abfs_path)
    print(f"--- DEBUG: Marca de agua actualizada a: {current_processing_timestamp} ---")
except Exception as update_e:
    print(f"--- ERROR: Falló la actualización final de la marca de agua: {update_e} ---")
    # This is a critical step for incremental loading. If it fails,
    # the next run might reprocess data. Log the error.
    pass # Continue execution


print("\nProceso Bronze a Silver completado exitosamente.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
