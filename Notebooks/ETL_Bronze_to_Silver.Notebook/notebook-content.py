# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "738a4ae6-13f4-4aca-b876-664aa7aaa6bc",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "0933622e-1b13-4fad-98cf-cac982f928b9",
# META       "known_lakehouses": [
# META         {
# META           "id": "738a4ae6-13f4-4aca-b876-664aa7aaa6bc"
# META         },
# META         {
# META           "id": "22a342f7-f9db-43b7-9bdb-134402aa63ff"
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
from pyspark.sql.functions import col, lit, coalesce, current_timestamp, date_format, max
from pyspark.sql.types import TimestampType
# import sys # Ya no necesitamos sys si removemos sys.exit

# Importar mssparkutils para operaciones del sistema de archivos en Fabric
from notebookutils import mssparkutils

# COMMAND ----------

# Definir las rutas a las capas Bronze y Silver en OneLake
# Asegúrate de que estos nombres de Lakehouse y la ruta ABFS sean correctos para tu entorno.
bronze_lakehouse_name = "" # Nombre del Lakehouse para la capa Bronze
silver_lakehouse_name = "" # Nombre del Lakehouse para la capa Silver
workspace_name = "" # **VERIFICADO DE LA SALIDA: AJUSTA SI ES NECESARIO**

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Construir las rutas base ABFS para los Lakehouses Bronze y Silver
bronze_layer_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_name}.Lakehouse/Tables/"
silver_layer_abfs_base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_name}.Lakehouse/Tables/"


# Definir las rutas a las tablas específicas en Bronze
bronze_orders_path = bronze_layer_abfs_base_path + "Orders"
bronze_orderitems_path = bronze_layer_abfs_base_path + "OrderItems"
bronze_customers_path = bronze_layer_abfs_base_path + "Customers"
bronze_products_path = bronze_layer_abfs_base_path + "Products"

# Definir la ruta a la tabla de destino en Silver
silver_salesorderlines_path = silver_layer_abfs_base_path + "SalesOrderLines_Silver"

# Definir la ruta para el archivo de marca de agua (high-watermark)
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

# Usamos una bandera para saber si debemos continuar
should_continue = True

# Añadimos try...except de nuevo, pero solo para establecer should_continue
try:
    df_orders_bronze = spark.read.format("delta").load(bronze_orders_path)
    print("--- DEBUG: Leído Bronze Orders ---")
except Exception as e:
    print(f"--- ERROR: Falló la lectura de Bronze Orders: {e} ---")
    should_continue = False # Establecer la bandera a False para detener la ejecución lógica


if should_continue:
    try:
        df_orderitems_bronze = spark.read.format("delta").load(bronze_orderitems_path)
        print("--- DEBUG: Leído Bronze OrderItems ---")
    except Exception as e:
        print(f"--- ERROR: Falló la lectura de Bronze OrderItems: {e} ---")
        should_continue = False # Establecer la bandera a False


if should_continue:
    try:
        df_customers_bronze = spark.read.format("delta").load(bronze_customers_path)
        print("--- DEBUG: Leído Bronze Customers ---")
    except Exception as e:
        print(f"--- ERROR: Falló la lectura de Bronze Customers: {e} ---")
        should_continue = False


if should_continue:
    try:
        df_products_bronze = spark.read.format("delta").load(bronze_products_path)
        print("--- DEBUG: Leído Bronze Products ---")
    except Exception as e:
        print(f"--- ERROR: Falló la lectura de Bronze Products: {e} ---")
        should_continue = False

# Solo imprimir esquemas si la lectura fue exitosa
if should_continue:
    print("\n--- DEBUG: Esquemas de tablas Bronze ---")
    df_orders_bronze.printSchema()
    df_orderitems_bronze.printSchema()
    df_customers_bronze.printSchema()
    df_products_bronze.printSchema()
else:
    print("\n--- ERROR: La lectura de una o más tablas de Bronze falló. Deteniendo el proceso. ---")


# COMMAND ----------

# Solo proceder si la lectura de Bronze fue exitosa
if should_continue:
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

         try:
             spark.createDataFrame([(last_processed_date,)], ["LastProcessedDate"]) \
                  .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
                  .write.format("delta").mode("overwrite").save(high_watermark_abfs_path)
             print("--- DEBUG: Archivo de marca de agua inicial creado. ---")
         except Exception as create_e:
                   print(f"--- ERROR: Falló la creación del archivo de marca de agua inicial: {create_e} ---")
                   # Si no puedes crear el archivo de marca de agua inicial, no deberías continuar
                        # ya que no podrás registrar el progreso.
                   should_continue = False # Detenemos la ejecución lógica si falla la creación inicial
                   pass # No lanzar excepción para que el notebook no falle de golpe aquí,
                        # pero la bandera should_continue lo detendrá.

    if should_continue: # Solo imprimir si la marca de agua fue leída o inicializada con éxito
        print(f"\n--- DEBUG DEPURACIÓN: Valor de last_processed_date ANTES de filtrar: {last_processed_date}")
        print(f"--- DEBUG DEPURACIÓN: Tipo de dato de last_processed_date: {type(last_processed_date)}")

    # Solo proceder a filtrar si la lectura de Bronze fue exitosa Y la marca de agua fue leída/inicializada
    if should_continue:
        print(f"--- DEBUG: Usando marca de agua: {last_processed_date} para filtrar datos de Bronze. ---")

        # --- 3. Filtrar datos nuevos usando la marca de agua ---
        print("\n--- DEBUG: Filtrando datos nuevos de Bronze ---")

        # Asegurarse de que OrderDate en Bronze es Timestamp para la comparación
        # Aunque el esquema ya lo mostró, es una validación extra si es necesario
        # df_orders_bronze = df_orders_bronze.withColumn("OrderDate", col("OrderDate").cast(TimestampType()))

        # Filtrar las órdenes en Bronze para obtener solo las nuevas desde la última marca de agua
        # Mantenemos ">" según tu lógica original. Si quisieras incluir datos con la misma fecha/hora, usa ">=".
        df_new_orders = df_orders_bronze.alias("o").filter(col("o.OrderDate") > last_processed_date)

        # Obtener los OrderID de las nuevas órdenes
        # Nota: Para datasets muy grandes, collect() puede ser ineficiente.
        # Una forma alternativa sería usar un semi-join:
        # df_new_orderitems = df_orderitems_bronze.join(df_new_orders.select("OrderID"), on="OrderID", how="semi")
        # Si el volumen es manejable, tu enfoque actual está bien.
        new_order_ids = df_new_orders.select("OrderID").distinct().rdd.flatMap(lambda x: x).collect()

        # Filtrar OrderItems para obtener solo los ítems relacionados con las nuevas órdenes
        df_new_orderitems = df_orderitems_bronze.alias("oi").filter(col("oi.OrderID").isin(new_order_ids))

        print(f"--- DEBUG: Conteo de nuevas órdenes después de filtrar por marca de agua: {df_new_orders.count()} ---")
        print(f"--- DEBUG: Conteo de nuevos ítems de orden después de filtrar por OrderID: {df_new_orderitems.count()} ---")


        # COMMAND ----------

        # Esto solo se ejecuta si la lectura de Bronze Y la lectura/inicialización del watermark fueron exitosas
        # --- 4. Procesar a Silver si hay nuevos datos ---
        print("\n--- DEBUG: Comprobando si hay nuevos datos para procesar a Silver ---")

        if df_new_orders.count() == 0:
            print("No hay nuevas órdenes para procesar en Bronze desde la última carga.")
            # Si no hay nuevos datos, actualizar la marca de agua con la fecha actual
            # Esto es útil si quieres que la próxima ejecución comience desde "ahora"
            # incluso si no hubo datos.
            print("\nActualizando la marca de agua (high-watermark) con la fecha/hora actual ya que no se encontraron nuevos datos...")
            current_processing_timestamp_no_data = datetime.datetime.now()
            try:
                spark.createDataFrame([(current_processing_timestamp_no_data,)], ["LastProcessedDate"]) \
                     .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
                     .write.format("delta").mode("overwrite").save(high_watermark_abfs_path)
                print(f"Marca de agua actualizada a: {current_processing_timestamp_no_data}")
            except Exception as update_e:
                print(f"--- ERROR: Falló la actualización de la marca de agua (sin datos): {update_e} ---")
                # Aquí podrías decidir si una falla al actualizar el watermark SIN datos es crítica.
                # Por ahora, solo lo registramos y el notebook terminará.
                pass

            print("Proceso Bronze a Silver finalizado (sin nuevos datos).")
            # La ejecución simplemente terminará la celda de forma limpia.

        else:
            print(f"Se encontraron {df_new_orders.count()} nuevas órdenes para procesar.")

            # --- 5. Unir, Limpiar y Transformar (De Bronze a Silver) ---
            print("\n--- DEBUG: Iniciando transformación a Silver ---")

            df_silver = df_new_orderitems.alias("oi") \
                .join(df_new_orders.alias("o"), on=(col("oi.OrderID") == col("o.OrderID")), how="inner") \
                .join(df_customers_bronze.alias("c"), on=(col("o.CustomerID") == col("c.CustomerID")), how="inner") \
                .join(df_products_bronze.alias("p"), on=(col("p.ProductID") == col("oi.ProductID")), how="inner") \
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
                     col("c.CustomerSegment").alias("CustomerSegment"),
                     col("p.ProductID").alias("ProductID_OLTP"),
                     col("p.ProductName").alias("ProductName"),
                     col("p.Category").alias("ProductCategory"),
                     col("p.Brand").alias("ProductBrand"),
                     col("oi.Quantity").alias("Quantity_Raw"),
                     col("oi.PricePerUnit").alias("PricePerUnit_Raw"),
                     col("oi.DiscountPct").alias("DiscountPct_Raw"),
                     col("oi.PromotionApplied").alias("PromotionApplied"),

                     # Limpieza y cálculos
                     coalesce(col("oi.Quantity"), lit(0)).cast("int").alias("Quantity"),
                     coalesce(col("oi.PricePerUnit"), lit(0)).cast("decimal(18,2)").alias("PricePerUnit"),
                     coalesce(col("oi.DiscountPct"), lit(0)).cast("decimal(5,2)").alias("DiscountPct"),


                     (
                        coalesce(col("oi.Quantity"), lit(0)).cast("decimal(18,4)") *
                        coalesce(col("oi.PricePerUnit"), lit(0)).cast("decimal(18,4)") *
                        (lit(1) - coalesce(col("oi.DiscountPct"), lit(0))/100)  # Aquí /100 es necesario
                     ).alias("SalesAmountLineItem"),

                     (
                         coalesce(col("oi.Quantity"), lit(0)).cast("decimal(18,4)") *
                         coalesce(col("oi.PricePerUnit"), lit(0)).cast("decimal(18,4)") *
                         coalesce(col("oi.DiscountPct"), lit(0)) /
                         lit(100)
                     ).alias("DiscountAmountLineItem"),

                     (
                        coalesce(col("oi.Quantity"), lit(0)).cast("decimal(18,2)") *
                        coalesce(col("oi.PricePerUnit"), lit(0)).cast("decimal(18,2)")
                     ).alias("GrossAmountLineItem"),

                     current_timestamp().alias("ProcessingTimestamp")
                 )

            print("\n--- DEBUG: Validación de cálculos ---")
            df_silver.select(
                    "DiscountPct",
                    "PricePerUnit",
                    "Quantity",
                    "DiscountAmountLineItem",
                    (col("PricePerUnit") * col("Quantity") * col("DiscountPct") / 100).alias("CalculadoManual")
                 ).filter(col("DiscountPct") > 0).show(10)

            print("\n--- DEBUG: Esquema del DataFrame Resultante (Capa Silver) ---")
            df_silver.printSchema()

            # COMMAND ----------

            # Esto solo se ejecuta si hay nuevos datos Y la lectura/inicialización del watermark fue exitosa
            # --- 6. Escribir datos a la capa Silver (Delta Lake) ---
            print("\n--- DEBUG: Escribiendo datos a la capa Silver ---")

            try:
                # Usar modo 'append' para añadir nuevos datos a la tabla Silver existente
                df_silver.write.format("delta").mode("append").save(silver_salesorderlines_path)
                print("--- DEBUG: Datos escritos a la capa Silver. ---")

                # COMMAND ----------

                # Esto solo se ejecuta si la escritura a Silver fue exitosa
                # --- 7. Actualizar la marca de agua (high-watermark) ---
                # Actualizar la marca de agua (high-watermark) con la fecha/hora MÁXIMA de los datos que SE ACABAN DE PROCESAR.
                print("\n--- DEBUG: Actualizando la marca de agua (high-watermark) con la fecha/hora MÁXIMA de los datos procesados... ---")

                # Calcular la marca de agua de los datos que se procesaron exitosamente
                # Obtenemos la fecha/hora máxima de OrderDate de las órdenes que acabamos de procesar.
                new_high_watermark = df_new_orders.agg(max("OrderDate")).collect()[0][0]

                # Si por alguna razón new_high_watermark es None (aunque no debería pasar si df_new_orders no está vacío),
                # usamos la marca de tiempo actual como respaldo.
                if new_high_watermark is None:
                     new_high_watermark = datetime.datetime.now()
                     print("--- ADVERTENCIA: No se pudo determinar la marca de agua a partir de los datos. Usando la marca de tiempo actual. ---")


                try:
                    spark.createDataFrame([(new_high_watermark,)], ["LastProcessedDate"]) \
                         .withColumn("LastProcessedDate", col("LastProcessedDate").cast(TimestampType())) \
                         .write.format("delta").mode("overwrite").save(high_watermark_abfs_path)
                    print(f"--- DEBUG: Marca de agua actualizada a: {new_high_watermark} ---")
                except Exception as update_e:
                     print(f"--- ERROR: Falló la actualización final de la marca de agua: {update_e} ---")
                     # Este es un paso crítico. Si falla, la próxima ejecución podría reprocesar datos.
                    # Deberías considerar qué hacer aquí: ¿fallar el notebook? ¿enviar una alerta?
                     pass # Por ahora, solo registramos el error.

                print("\nProceso Bronze a Silver completado exitosamente.")

            except Exception as write_e:
                print(f"--- ERROR: Falló la escritura a la capa Silver: {write_e} ---")
                # Si la escritura a Silver falla, NO debemos actualizar la marca de agua final.
                # Esto es CRÍTICO para evitar perder datos.
                # Aquí puedes agregar lógica adicional, como enviar una alerta o registrar la falla de forma más robusta.
                should_continue = False # Establecer bandera a False, aunque el notebook ya puede estar terminando.
                pass # Dejar que el notebook termine con error si es el comportamiento deseado.

# Este else se ejecuta si should_continue se volvió False en alguna etapa inicial (lectura Bronze o inicialización watermark)
else:
    print("\nProceso Bronze a Silver finalizado debido a errores en la lectura de Bronze o inicialización del watermark.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
