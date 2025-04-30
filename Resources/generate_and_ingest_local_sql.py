# =================================================================
# Script Python para Generar Datos y Cargar Directamente a SQL Server OLTP
# Este script se ejecuta LOCALMENTE (fuera de Microsoft Fabric)
# Lee credenciales sensibles de un archivo .env usando python-dotenv.
# =================================================================

# Importar las librerías necesarias
import pyodbc
import random
import datetime
from faker import Faker
import pandas as pd
import sys
import time
import os # Importar la librería os para leer variables de entorno
from dotenv import load_dotenv # Importar load_dotenv para leer desde .env

# =================================================================
# --- Configuración de la Conexión a SQL Server Local ---
# =================================================================
# Cargar variables del archivo .env si existe
# Asegúrate de tener un archivo .env en la misma carpeta que este script
# y de haber instalado la librería python-dotenv (pip install python-dotenv).
load_dotenv()

# Lee los detalles de conexión de variables de entorno (ahora cargadas desde .env o el entorno del sistema).
# Asegúrate de que estas variables estén configuradas en tu archivo .env o en tu sistema antes de ejecutar el script.
# Ejemplo de contenido de .env:
# SQL_SERVER_NAME=.
# SQL_DATABASE_NAME=RetailNova
# SQL_USERNAME=sa
# SQL_PASSWORD=TuContraseñaSegura
# SQL_DRIVER={ODBC Driver 17 for SQL Server}

sql_server_name = os.environ.get("SQL_SERVER_NAME", ".") # Lee de env var, usa "." por defecto si no está seteada
sql_database_name = os.environ.get("SQL_DATABASE_NAME", "RetailNova") # Lee de env var, usa "RetailNova" por defecto
sql_username = os.environ.get("SQL_USERNAME") # **DEBE** estar seteada en .env o env var
sql_password = os.environ.get("SQL_PASSWORD") # **DEBE** estar seteada en .env o env var

# Puedes necesitar especificar el driver ODBC.
# Lee de env var, usa un valor común por defecto.
sql_driver = os.environ.get("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}") # Lee de env var, usa un valor común por defecto

# --- VALIDACIÓN DE VARIABLES DE ENTORNO ---
if not sql_username or not sql_password:
    print("--- ERROR: Las variables de entorno SQL_USERNAME y SQL_PASSWORD deben estar configuradas en el archivo .env o en el entorno del sistema. ---")
    print("Por favor, crea o actualiza tu archivo .env con estas variables.")
    sys.exit(1) # Salir si las credenciales no están configuradas

# Cadena de conexión
cnxn_str = f"DRIVER={sql_driver};SERVER={sql_server_name};DATABASE={sql_database_name};UID={sql_username};PWD={sql_password}"

print("--- Configuración de conexión a SQL Server definida (leyendo de variables de entorno/archivo .env). ---")
# Opcional: Imprimir la cadena de conexión (sin la contraseña por seguridad en logs)
print(f"--- Cadena de conexión (sin PWD): DRIVER={sql_driver};SERVER={sql_server_name};DATABASE={sql_database_name};UID={sql_username} ---")


# =================================================================
# --- Parámetros de Generación de Datos ---
# =================================================================
num_customers_to_generate = 1000 # Número total de clientes fijos a generar
num_products_to_generate = 50 # Número total de productos fijos a generar
num_orders_to_generate = 500 # Número de órdenes a generar en CADA EJECUCIÓN del script
max_items_per_order = 5
max_quantity_per_item = 10

fake = Faker()

print(f"--- Parámetros de generación de datos: {num_orders_to_generate} órdenes a generar por ejecución. ---")


# =================================================================
# --- Función para Generar Datos en Memoria ---
# =================================================================
def generate_ecommerce_data_in_memory(num_orders, fixed_customers, fixed_products):
    """
    Genera datos de e-commerce aleatorios directamente en memoria.

    Args:
        num_orders (int): Número de órdenes a generar.
        fixed_customers (list): Lista de tuplas (CustomerID, CustomerName, CustomerSegment) para referenciar.
        fixed_products (list): Lista de tuplas (ProductID, ProductName, Category, Brand, Price) para referenciar.

    Returns:
        tuple: Dos DataFrames de Pandas (df_orders, df_order_items).
    """
    print("\n--- Iniciando generación de datos en memoria ---")

    orders_data = []
    order_items_data = []

    order_id_counter = 1 # Contador para IDs de orden simulados
    order_item_id_counter = 1 # Contador para OrderItemKey simulados

    # Listas de valores posibles para atributos categóricos
    device_types = ['Desktop', 'Mobile', 'Tablet']
    referral_sources = ['Search Engine', 'Social Media', 'Direct', 'Referral']
    shipping_methods = ['Standard', 'Express', 'Next Day']
    order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
    promotions = ['PROMO_SPRING', 'PROMO_SUMMER', 'PROMO_WEEKEND', None]

    # --- Usar el timestamp UTC actual como base para las fechas de las nuevas órdenes ---
    current_datetime_for_run_utc = datetime.datetime.now(datetime.timezone.utc)
    # --- FIN MODIFICACIÓN ---

    for i in range(num_orders):
        # --- Generar OrderDate basado en el timestamp UTC actual + pequeño delta ---
        # Esto asegura que los nuevos timestamps sean > que la última marca de agua del pipeline de Fabric (en UTC)
        order_date = current_datetime_for_run_utc + datetime.timedelta(microseconds=i * 10) # Añadir microsegundos para unicidad dentro del lote
        # --- FIN MODIFICACIÓN ---

        customer = random.choice(fixed_customers) # Seleccionar un cliente existente
        customer_source_id = customer[0] # Usamos el ID de origen del cliente fijo

        # Generar un OrderSourceID único para esta ejecución
        # Combinamos un prefijo, un timestamp y un número aleatorio
        order_source_id_simulated = f"ORDER_{int(time.time() * 1000)}_{order_id_counter}"

        session_id = fake.uuid4()
        device_type = random.choice(device_types)
        referral_source = random.choice(referral_sources)
        shipping_method = random.choice(shipping_methods)
        order_status = random.choice(order_statuses)
        shipping_city = fake.city() # Ciudad de envío
        payment_method = random.choice(payment_methods)
        transaction_id = fake.uuid4()
        promotion_applied = random.choice(promotions)

        orders_data.append({
            "order_id": order_source_id_simulated, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "order_date": order_date, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "customer_id": customer_source_id, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "customer_name": customer[1], # <-- Usar nombre de columna de staging (lowercase_underscore) (desde la tupla fixed_customers)
            "customer_segment": customer[2], # <-- Usar nombre de columna de staging (lowercase_underscore) (desde la tupla fixed_customers)
            "session_id": session_id, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "device_type": device_type, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "referral_source": referral_source, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "shipping_method": shipping_method, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "order_status": order_status, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "shipping_city": shipping_city, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "payment_method": payment_method, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "transaction_id": transaction_id, # <-- Usar nombre de columna de staging (lowercase_underscore)
            "promotion_applied": promotion_applied # <-- Usar nombre de columna de staging (lowercase_underscore)
        })

        num_items = random.randint(1, max_items_per_order)
        for j in range(num_items): # Usar 'j' para evitar conflicto con 'i' del bucle exterior
            product = random.choice(fixed_products) # Seleccionar un producto existente
            product_id_oltp = product[0] # Usamos el ID de origen del producto fijo
            product_name = product[1]
            product_category = product[2]
            product_brand = product[3]
            price_per_unit = product[4]
            quantity = random.randint(1, max_quantity_per_item)
            discount_pct = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15]), 2) # Simula algunos descuentos

            order_items_data.append({
                "OrderItemKey_Simulated": order_item_id_counter, # Clave única simulada para la línea de pedido (NO es una columna de staging)
                "order_id": order_source_id_simulated, # <-- Relacionar con la orden simulada usando nombre de columna de staging (lowercase_underscore)
                "product_id": product_id_oltp, # <-- Usar nombre de columna de staging (lowercase_underscore)
                "product_name": product_name, # <-- Usar nombre de columna de staging (lowercase_underscore)
                "category": product_category, # <-- Usar nombre de columna de staging (lowercase_underscore)
                "brand": product_brand, # <-- Usar nombre de columna de staging (lowercase_underscore)
                "quantity": quantity, # <-- Usar nombre de columna de staging (lowercase_underscore)
                "price": price_per_unit, # <-- Usar nombre de columna de staging (lowercase_underscore)
                "discount_pct": discount_pct # <-- Usar nombre de columna de staging (lowercase_underscore)
            })
            order_item_id_counter += 1
        order_id_counter += 1

    df_orders = pd.DataFrame(orders_data)
    df_order_items = pd.DataFrame(order_items_data)

    print(f"--- Generados {len(orders_data)} órdenes y {len(order_items_data)} líneas de pedido en memoria. ---")

    return df_orders, df_order_items


# =================================================================
# --- Lógica Principal de Ejecución ---
# =================================================================
if __name__ == "__main__":
    print("--- Iniciando script de generación e ingesta local a SQL Server ---")

    cnxn = None # Inicializar conexión como None

    try:
        # --- 1. Establecer Conexión a SQL Server ---
        print("\n--- Conectando a SQL Server ---")
        # La conexión ahora usa las variables leídas de entorno/archivo .env
        cnxn = pyodbc.connect(cnxn_str)
        cursor = cnxn.cursor()
        print("--- Conexión a SQL Server establecida. ---")

        # --- 2. Crear Estructura OLTP (si no existe) ---
        # Incluimos la lógica de creación de tablas OLTP aquí para que el script sea autónomo para setup.
        print("\n--- Creando estructura OLTP (si no existe) ---")

        create_oltp_structure_sql = """
        -- Crear un esquema para organizar las tablas OLTP simuladas
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'OLTP')
        BEGIN
            EXEC('CREATE SCHEMA OLTP');
            PRINT 'Esquema OLTP creado.';
        END;

        -- Eliminar tablas OLTP si ya existen (útil para re-ejecutar durante desarrollo/prueba)
        -- En un escenario de producción, no harías esto en cada ejecución de ingesta.
        IF OBJECT_ID('OLTP.OrderItems', 'U') IS NOT NULL DROP TABLE OLTP.OrderItems;
        IF OBJECT_ID('OLTP.Orders', 'U') IS NOT NULL DROP TABLE OLTP.Orders;
        IF OBJECT_ID('OLTP.Products', 'U') IS NOT NULL DROP TABLE OLTP.Products;
        IF OBJECT_ID('OLTP.Customers', 'U') IS NOT NULL DROP TABLE OLTP.Customers;

        PRINT 'Tablas OLTP simuladas eliminadas (si existían).';

        -- 1. Crear Tabla de Clientes (OLTP.Customers)
        PRINT 'Creando tabla OLTP.Customers...';
        CREATE TABLE OLTP.Customers (
            CustomerID INT IDENTITY(1,1) PRIMARY KEY,
            CustomerSourceID VARCHAR(50) NOT NULL UNIQUE, -- Asegura que el ID de origen es único en esta tabla
            CustomerName VARCHAR(255),
            CustomerSegment VARCHAR(50)
        );
        PRINT 'Tabla OLTP.Customers creada.';

        -- 2. Crear Tabla de Productos (OLTP.Products)
        PRINT 'Creando tabla OLTP.Products...';
        CREATE TABLE OLTP.Products (
            ProductID INT IDENTITY(1,1) PRIMARY KEY,
            ProductID_OLTP VARCHAR(50) NOT NULL UNIQUE, -- ADDED: Columna para almacenar el ID de origen del producto (ej. PROD_001)
            ProductName VARCHAR(255), -- ProductName ya no necesita ser UNIQUE si ProductID_OLTP es la BK
            Category VARCHAR(50),
            Brand VARCHAR(50)
        );
        PRINT 'Tabla OLTP.Products creada.';

        -- 3. Crear Tabla de Pedidos (OLTP.Orders)
        PRINT 'Creando tabla OLTP.Orders...';
        CREATE TABLE OLTP.Orders (
            OrderID INT IDENTITY(1,1) PRIMARY KEY,
            OrderSourceID VARCHAR(50) NOT NULL UNIQUE, -- Asegura que el ID de origen del pedido es único
            CustomerID INT NOT NULL, -- FK a OLTP.Customers
            OrderDate DATETIME2 NOT NULL, -- Usamos DATETIME2 para precisión de timestamp
            SessionID VARCHAR(100),
            DeviceType VARCHAR(50),
            ReferralSource VARCHAR(255),
            ShippingMethod VARCHAR(100),
            OrderStatus VARCHAR(50),
            ShippingCity VARCHAR(50),
            PaymentMethod VARCHAR(50),
            TransactionID VARCHAR(50),
            CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) REFERENCES OLTP.Customers(CustomerID)
        );
        PRINT 'Tabla OLTP.Orders creada.';

        -- 4. Crear Tabla de Items de Pedido (OLTP.OrderItems)
        PRINT 'Creando tabla OLTP.OrderItems...';
        CREATE TABLE OLTP.OrderItems (
            OrderItemID INT IDENTITY(1,1) PRIMARY KEY,
            OrderID INT NOT NULL, -- FK a OLTP.Orders
            ProductID INT NOT NULL, -- FK a OLTP.Products
            Quantity INT,
            PricePerUnit DECIMAL(18, 2), -- Ajustamos al tipo numérico
            DiscountPct DECIMAL(5, 2), -- Ajustamos al tipo numérico para porcentaje
            PromotionApplied VARCHAR(50),
            CONSTRAINT FK_OrderItems_Orders FOREIGN KEY (OrderID) REFERENCES OLTP.Orders(OrderID),
            CONSTRAINT FK_OrderItems_Products FOREIGN KEY (ProductID) REFERENCES OLTP.Products(ProductID)
        );
        PRINT 'Tabla OLTP.OrderItems creada.';

        PRINT 'Esquema OLTP simulado creado exitosamente.';
        """
        # Ejecutar el script de creación de estructura OLTP
        cursor.execute(create_oltp_structure_sql)
        cnxn.commit()


        # --- 3. Generar un conjunto fijo de Clientes y Productos (simulación de datos maestros) ---
        # En un escenario real, leerías estos datos de tus tablas OLTP existentes
        # Generamos IDs de origen con formato para que ProductID_OLTP en la tabla OLTP.Products pueda almacenarlos.
        print("\n--- Generando conjunto fijo de clientes y productos para referencia ---")
        fixed_customers = [(f"CUST_{i+1:04d}", fake.name(), random.choice(['Individual', 'Business', 'Premium'])) for i in range(num_customers_to_generate)] # Usar formato CUST_0001
        fixed_products = [(f"PROD_{i+1:03d}", fake.word().capitalize() + ' Product', random.choice(['Electronics', 'Books', 'Clothing', 'Home Goods']), random.choice(['BrandA', 'BrandB', 'BrandC']), round(random.uniform(10, 500), 2)) for i in range(num_products_to_generate)] # Usar formato PROD_001
        print(f"--- Generados {len(fixed_customers)} clientes fijos y {len(fixed_products)} productos fijos. ---")


        # --- 4. Generar datos de Órdenes y OrderItems en memoria ---
        # Ya no pasamos start_date y end_date a la función de generación
        df_orders, df_order_items = generate_ecommerce_data_in_memory(
            num_orders_to_generate,
            fixed_customers, # Pasar los clientes fijos para referenciar
            fixed_products # Pasar los productos fijos para referenciar
        )

        # --- 5. Crear Tabla de Staging si no existe ---
        print("\n--- Creando tabla de staging dbo.RetailNova_data si no existe ---")
        create_staging_table_sql = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'RetailNova_data')
        BEGIN
            CREATE TABLE dbo.RetailNova_data (
                order_id VARCHAR(50),
                customer_id VARCHAR(50),
                customer_name VARCHAR(255),
                order_date DATETIME2, -- Usamos DATETIME2 para precisión de timestamp
                product_id VARCHAR(50),
                product_name VARCHAR(255),
                category VARCHAR(50),
                brand VARCHAR(50),
                price DECIMAL(18, 2),
                quantity INT,
                discount_pct DECIMAL(5, 2),
                shipping_city VARCHAR(50),
                payment_method VARCHAR(50),
                transaction_id VARCHAR(50),
                customer_segment VARCHAR(50),
                promotion_applied VARCHAR(50),
                session_id VARCHAR(100),
                device_type VARCHAR(50),
                referral_source VARCHAR(255),
                shipping_method VARCHAR(100),
                order_status VARCHAR(50)
                -- No PRIMARY KEY o UNIQUE constraints en staging, es solo una tabla temporal
            );
            PRINT 'Tabla dbo.RetailNova_data creada.';
        END
        ELSE
        BEGIN
            PRINT 'Tabla dbo.RetailNova_data ya existe.';
        END;
        """
        cursor.execute(create_staging_table_sql)
        cnxn.commit()
        print("--- Proceso de creación de tabla de staging completado. ---")


        # --- 6. Limpiar Tabla de Staging ---
        print("\n--- Truncando tabla de staging dbo.RetailNova_data ---")
        try:
            cursor.execute("TRUNCATE TABLE dbo.RetailNova_data;")
            cnxn.commit()
            print("--- Tabla de staging truncada. ---")
        except pyodbc.ProgrammingError as e:
             print(f"--- Advertencia: Falló TRUNCATE TABLE (la tabla podría no existir). Intentando DELETE. Error: {e} ---")
             try:
                 cursor.execute("DELETE FROM dbo.RetailNova_data;")
                 cnxn.commit()
                 print("--- Tabla de staging limpiada con DELETE. ---")
             except Exception as delete_e:
                 print(f"--- ERROR: Falló DELETE FROM dbo.RetailNova_data. Asegúrate de que la tabla existe. Error: {delete_e} ---")
                 raise # Re-lanzar el error si falla la limpieza


        # --- 7. Insertar datos generados en la Tabla de Staging ---
        print("\n--- Insertando datos en la tabla de staging dbo.RetailNova_data ---")

        # Preparar la sentencia INSERT para la tabla de staging
        # Asegúrate de que los nombres de las columnas coincidan con tu tabla dbo.RetailNova_data
        # Y que el orden de las columnas en la lista coincida con la sentencia INSERT.
        # ¡CORREGIDO! Nombres de columna ajustados para coincidir con dbo.RetailNova_data (lowercase_underscore)
        insert_staging_sql = """
        INSERT INTO dbo.RetailNova_data (
            order_id, customer_id, customer_name, order_date, product_id, product_name,
            category, brand, price, quantity, discount_pct, shipping_city, payment_method,
            transaction_id, customer_segment, promotion_applied, session_id, device_type,
            referral_source, shipping_method, order_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Insertar datos de órdenes y order_items en la tabla de staging combinada
        # Creamos un DataFrame combinado para la inserción
        # Asegúrate de que las columnas de unión ("order_id") y las columnas seleccionadas
        # tengan los nombres correctos según tus DataFrames generados.
        df_staging_data = df_order_items.merge(df_orders, on="order_id", how="inner") # <-- CORREGIDO: Usar "order_id" para la unión

        # Seleccionar y reordenar columnas para que coincidan con la sentencia INSERT
        # ¡CORREGIDO! Nombres de columna y orden ajustados para coincidir con insert_staging_sql y dbo.RetailNova_data
        df_staging_data = df_staging_data[[
            "order_id", "customer_id", "customer_name", "order_date", "product_id", "product_name",
            "category", "brand", "price", "quantity", "discount_pct", "shipping_city", "payment_method",
            "transaction_id", "customer_segment", "promotion_applied", "session_id", "device_type",
            "referral_source", "shipping_method", "order_status"
        ]]

        # Convertir DataFrame a lista de tuplas para la inserción masiva
        # Asegúrate de que los tipos de datos en el DataFrame sean compatibles con los tipos de columna en SQL Server.
        # pyodbc generalmente maneja tipos comunes como int, float, string, datetime.
        data_to_insert = [tuple(row) for row in df_staging_data.values]

        # Ejecutar inserción masiva
        # executemany es eficiente para insertar muchas filas a la vez.
        cursor.executemany(insert_staging_sql, data_to_insert)
        cnxn.commit()
        print(f"--- Insertados {len(data_to_insert)} registros en la tabla de staging. ---")


        # --- 8. Ejecutar Sentencias SQL para Poblar Tablas OLTP desde Staging ---
        # Estas sentencias leen de la tabla de staging y escriben en las tablas OLTP.
        # Asegúrate de que estas sentencias coincidan con la estructura de tus tablas OLTP
        # y usen los nombres de columna correctos de la tabla dbo.RetailNova_data.

        print("\n--- Poblando tablas OLTP desde staging ---")

        # Script SQL para poblar OLTP.Customers y OLTP.Products (INSERT NOT EXISTS)
        # Adaptado de script_populate_oltp_final_casing (Parte 1)
        populate_customers_products_sql = """
        PRINT 'Poblando OLTP.Customers y OLTP.Products...';

        -- 1. Insertar datos en OLTP.Customers
        PRINT 'Insertando datos en OLTP.Customers usando INSERT NOT EXISTS con GROUP BY...';
        INSERT INTO OLTP.Customers (CustomerSourceID, CustomerName, CustomerSegment)
        -- ¡CORREGIDO! Usando nombres de columna de dbo.RetailNova_data: customer_id, customer_name, customer_segment
        SELECT d.[customer_id], MAX(d.[customer_name]), MAX(d.[customer_segment]) -- Usando nombres de columna de dbo.RetailNova_data
        FROM dbo.RetailNova_data AS d
        WHERE d.[customer_id] IS NOT NULL
        GROUP BY d.[customer_id]
        HAVING COUNT(d.[customer_id]) > 0
        AND NOT EXISTS (
            SELECT 1
            FROM OLTP.Customers AS target
            WHERE target.CustomerSourceID = d.[customer_id] -- ¡CORREGIDO! Usando customer_id
        );
        PRINT 'Datos insertados en OLTP.Customers.';


        -- 2. Insertar datos en OLTP.Products
        PRINT 'Insertando datos en OLTP.Products usando INSERT NOT EXISTS con GROUP BY...';
        INSERT INTO OLTP.Products (ProductID_OLTP, ProductName, Category, Brand)
        -- Usando nombres de columna de dbo.RetailNova_data: product_id, product_name, category, brand
        SELECT d.[product_id], MAX(d.[product_name]), MAX(d.[category]), MAX(d.[brand]) -- Usando nombres de columna de dbo.RetailNova_data
        FROM dbo.RetailNova_data AS d
        WHERE d.[product_id] IS NOT NULL -- Usar product_id como clave de negocio para unicidad
        GROUP BY d.[product_id]
        HAVING COUNT(d.[product_id]) > 0
        AND NOT EXISTS (
            SELECT 1
            FROM OLTP.Products AS target
            WHERE target.ProductID_OLTP = d.[product_id] -- ¡CORREGIDO! Comparar con ProductID_OLTP en la tabla OLTP usando product_id de staging
        );
        PRINT 'Datos insertados en OLTP.Products.';

        PRINT 'Población de OLTP.Customers y OLTP.Products completada.';
        """
        # Ejecutar el script de población de Clientes y Productos
        cursor.execute(populate_customers_products_sql)
        cnxn.commit()


        # Script SQL para poblar OLTP.Orders y OLTP.OrderItems (MERGE/INSERT)
        # Adaptado de script_populate_oltp_final_casing (Parte 2)
        # NOTA: Este script asume que los CustomerID y ProductID ya existen en las tablas OLTP
        # después de ejecutar el script anterior.
        populate_orders_orderitems_sql = """
        PRINT 'Poblando OLTP.Orders y OLTP.OrderItems...';

        -- --- DEBUG: Verificación de filas con promociones en staging y su unión ---
        -- Contar filas en staging con promociones no nulas
        DECLARE @StagingRowsWithPromo INT;
        SELECT @StagingRowsWithPromo = COUNT(*) FROM dbo.RetailNova_data WHERE promotion_applied IS NOT NULL;
        PRINT 'DEBUG: Filas en staging con promotion_applied NO NULL: ' + CAST(@StagingRowsWithPromo AS NVARCHAR(10));

        -- Contar filas de staging con promociones no nulas que se unen a OLTP.Orders y OLTP.Products
        DECLARE @JoinedRowsWithPromo INT;
        SELECT @JoinedRowsWithPromo = COUNT(d.order_id)
        FROM dbo.RetailNova_data AS d
        JOIN OLTP.Orders AS o ON d.[order_id] = o.OrderSourceID
        -- ¡CORREGIDO! Unir a OLTP.Products usando product_id de staging y ProductID_OLTP de la tabla OLTP
        JOIN OLTP.Products AS p ON d.[product_id] = p.ProductID_OLTP
        WHERE d.promotion_applied IS NOT NULL;
        PRINT 'DEBUG: Filas de staging con promotion_applied NO NULL que se unen correctamente: ' + CAST(@JoinedRowsWithPromo AS NVARCHAR(10));
        -- --- FIN DEBUG ---

        -- --- DEBUG: Inspeccionar algunas filas de staging con promociones ---
        PRINT 'DEBUG: Primeras 10 filas en staging con promotion_applied NO NULL:';
        SELECT TOP 10
            order_id,
            product_name,
            promotion_applied
        FROM dbo.RetailNova_data
        WHERE promotion_applied IS NOT NULL;
        PRINT '--- FIN DEBUG SELECT ---';
        -- --- FIN DEBUG SELECT ---


        -- 3. Insertar datos en OLTP.Orders
        PRINT 'Insertando datos en OLTP.Orders...';
        -- Nota: Necesitamos unir con la tabla OLTP.Customers para obtener el CustomerID basado en CustomerSourceID
        MERGE INTO OLTP.Orders AS target
        USING (
            SELECT DISTINCT
                -- ¡CORREGIDO! Usando nombres de columna EXACTOS de dbo.RetailNova_data!
                d.[order_id] AS OrderSourceID,
                c.CustomerID, -- Obtenemos el CustomerID de la tabla OLTP.Customers
                d.[order_date] AS OrderDate,
                d.[session_id] AS SessionID,
                d.[device_type] AS DeviceType,
                d.[referral_source] AS ReferralSource,
                d.[shipping_method] AS ShippingMethod,
                d.[order_status] AS OrderStatus,
                d.[shipping_city] AS ShippingCity,
                d.[payment_method] AS PaymentMethod,
                d.[transaction_id] AS TransactionID
            FROM dbo.RetailNova_data AS d
            JOIN OLTP.Customers AS c ON d.[customer_id] = c.CustomerSourceID -- ¡CORREGIDO! Usando customer_id
            WHERE d.[order_id] IS NOT NULL AND d.[customer_id] IS NOT NULL AND d.[order_date] IS NOT NULL -- ¡CORREGIDO! Usando nombres de columna
        ) AS source
        ON (target.OrderSourceID = source.OrderSourceID)
        WHEN NOT MATCHED THEN
            INSERT (OrderSourceID, CustomerID, OrderDate, SessionID, DeviceType, ReferralSource, ShippingMethod, OrderStatus, ShippingCity, PaymentMethod, TransactionID)
            VALUES (source.OrderSourceID, source.CustomerID, source.OrderDate, source.SessionID, source.DeviceType, source.ReferralSource, source.ShippingMethod, source.OrderStatus, source.ShippingCity, source.PaymentMethod, source.TransactionID);
        PRINT 'Datos insertados en OLTP.Orders.';


        -- 4. Insertar datos en OLTP.OrderItems
        PRINT 'Insertando datos en OLTP.OrderItems...';
        -- Nota: Necesitamos unir con OLTP.Orders y OLTP.Products para obtener OrderID y ProductID
        MERGE INTO OLTP.OrderItems AS target
        USING (
            SELECT
                o.OrderID, -- Obtenemos el OrderID de la tabla OLTP.Orders
                p.ProductID, -- Obtenemos el ProductID de la tabla OLTP.Products
                -- ¡CORREGIDO! Usando nombres de columna EXACTOS de dbo.RetailNova_data!
                d.[quantity] AS Quantity,
                d.[price] AS PricePerUnit, -- Usamos 'price' del staging como PricePerUnit
                d.[discount_pct] AS DiscountPct,
                d.[promotion_applied] AS PromotionApplied -- <-- ADDED PromotionApplied column from staging
            FROM dbo.RetailNova_data AS d
            JOIN OLTP.Orders AS o ON d.[order_id] = o.OrderSourceID -- ¡CORREGIDO! Usando order_id
            -- ¡CORREGIDO! Unión a OLTP.Products por ProductID_OLTP usando d.[product_id]
            JOIN OLTP.Products AS p ON d.[product_id] = p.ProductID_OLTP
            WHERE d.[order_id] IS NOT NULL AND d.[product_id] IS NOT NULL -- ¡CORREGIDO! Usando nombres de columna
        ) AS source
        ON (1=0) -- Usamos una condición ON falsa para force INSERT-only (no hay clave natural para MERGE here)
        WHEN NOT MATCHED THEN
            INSERT (OrderID, ProductID, Quantity, PricePerUnit, DiscountPct, PromotionApplied) -- <-- ADDED PromotionApplied column
            VALUES (source.OrderID, source.ProductID, source.Quantity, source.PricePerUnit, source.DiscountPct, source.PromotionApplied); -- <-- ADDED PromotionApplied value
        -- Nota: If you need idempotency for OrderItems, you would need to add a unique key
        -- or use a different approach (e.g. truncate and load, or use a hash of relevant columns)
        PRINT 'Datos insertados en OLTP.OrderItems.';

        PRINT 'Esquema OLTP simulado poblado exitosamente.';
        """
        # Ejecutar el script de población de Órdenes y OrderItems
        cursor.execute(populate_orders_orderitems_sql)
        cnxn.commit()


        print("\n--- Proceso de generación e ingesta local a SQL Server completado exitosamente. ---")

    except Exception as e:
        print(f"--- ERROR: Falló el proceso de ingesta a SQL Server: {e} ---")
        if cnxn:
            cnxn.rollback() # Revertir cambios si hay error
            print("--- Transacción revertida. ---")
        sys.exit(1) # Salir con código de error

    finally:
        # Cerrar conexión
        if cnxn:
            cnxn.close()
            print("--- Conexión a SQL Server cerrada. ---")

