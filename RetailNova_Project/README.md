# 🚀 RetailNova Data Pipeline Project

This repository contains the scripts and configuration needed to simulate a local transactional data source (OLTP) and process it through an ingestion and transformation pipeline in Microsoft Fabric, building a dimensional model in a Lakehouse.
The project simulates a sales data flow from an e-commerce system (RetailNova) and demonstrates an incremental loading approach using high-watermarks in Microsoft Fabric.

## 📦 Repository Contents
* generate_and_ingest_local_sql.py: Python script to generate simulated sales data and load it into a local SQL Server database. This script also creates the simulated OLTP structure and a temporary staging table.
* .env.example: An example file showing the structure of the .env file needed to configure the connection to local SQL Server. This file does NOT contain sensitive credentials.
* .gitignore: File to specify which files and folders Git should ignore, including the actual .env file.
* Additional SQL scripts (if any, list them here, although the Python script already includes them internally).
* Fabric pipeline configuration files or exports (if included).

## ✅ Prerequisites
To set up and run this project, you will need the following:
1. Microsoft SQL Server: An instance of SQL Server (Express, Developer, Standard, etc.) installed locally or accessible from your machine.
2. Python 3.x: Python installed on your local machine.
3. ODBC Driver for SQL Server: An ODBC driver compatible with your SQL Server version installed on your local machine (e.g., "ODBC Driver 17 for SQL Server" or "ODBC Driver 18 for SQL Server").
4. Python Libraries: Install the necessary libraries (pyodbc, faker, pandas, python-dotenv). You can install them using pip:

       pip install pyodbc faker pandas python-dotenv

6. Access to Microsoft Fabric: A Microsoft Fabric account with active capacity.
7. On-premises data gateway: Installed and configured on the same machine as your local SQL Server. This gateway will allow Fabric to connect to your local SQL Server database.
# ⚙️ Environment Setup
Follow these steps to set up the environment:
### 1. 💾 Local SQL Server Database Setup
* Open SQL Server Management Studio (SSMS).
* Connect to your local SQL Server instance.
* Create a new database named RetailNova.
* CREATE DATABASE RetailNova;
* Ensure that the SQL Server user (e.g., sa) you will use has permissions to create tables, insert, truncate, and drop in this database.

### 2. 🐍 Local Python Script Setup (generate_and_ingest_local_sql.py)
* This script reads SQL Server connection details from environment variables, which are loaded from a local .env file by the python-dotenv library.
* Download the generate_and_ingest_local_sql.py and .env.example files from this repository.
* Create your .env file: Make a copy of the .env.example file and rename it to .env in the same folder as generate_and_ingest_local_sql.py.
* Edit your .env file: Open the .env file and update the values with your local SQL Server connection details:

       SQL_SERVER_NAME=. # Replace with your local SQL server name or IP
       SQL_DATABASE_NAME=RetailNova
       SQL_USERNAME=sa # Replace with your SQL user
       SQL_PASSWORD=YourSecurePassword # Replace with your SQL password
       SQL_DRIVER={ODBC Driver 17 for SQL Server} # Replace with the exact name of your installed ODBC driver

* Configure .gitignore: Make sure your .gitignore file (in the root of your Git repository) includes the line .env to prevent accidentally uploading your sensitive credentials. A basic .gitignore for this project might look like this:

### Files and folders to ignore
       .env 
       __pycache__/ 
       *.pyc

* Save both files (.env and .gitignore).

**Important Note:** To allow the initial synchronization to complete without errors related to external data source connections, the activities within the Full_Process pipeline in this repository have been intentionally disabled. This facilitates import but will require an additional configuration step.
### 3. ☁️ Microsoft Fabric Setup
* Access your Microsoft Fabric environment.
* Sync the Repository with your Fabric Workspace:
  * In the workspace settings, go to Git integration.
  * Connect your workspace to the cloned repository, selecting the appropriate branch and folder. Start the synchronization process (Sync). Upon synchronization, the Lakehouses (LH_Bronze, LH_Silver, LH_Gold), the Pipeline (Full_Process), and the Notebooks (ETL_Bronze_to_Silver, ETL_Silver_to_Gold) defined in the repository will be automatically created in your workspace.
Important Note: To allow the initial synchronization to complete without errors related to external data source connections, the activities within the Full_Process pipeline in this repository have been **intentionally disabled**. This facilitates import but will require an additional configuration step.

### ➡️ Full_Process Pipeline Configuration
Once the Full_Process pipeline has been synchronized to your workspace (with its activities disabled), you will need to enable them and configure the necessary connections.
* Open the Full_Process pipeline in your Fabric workspace. 🖱️ 
* You will notice that the activities are disabled (grey icon). 🚫 
* **Enable each of the activities** by right-clicking on them and selecting Enable. ✅ 
Now, configure each pipeline activity in detail:

### Overall Pipeline Visualization:
  
#### Activity: Lookup (Get OLTP Tables)
This activity is used to get the list of tables in the OLTP schema of your local SQL Server database that will be processed in the next step (ForEach).
* Click on the **Lookup** activity (likely named something like Lookup OLTP Tables). 🔍
* **General tab:**
  * Verify the activity's **Name** (e.g., Lookup OLTP Tables).
  * Ensure it is **Enabled**.
 * **Settings** tab:
    * **Connection**: Select the connection to your local SQL Server that you created previously (e.g., OnPremSQLGateway). **This name must exactly match the name of your connection in Fabric.**
    * **Connection type**: SQL Server
    * **Database**: Select or type the name of your database: RetailNova.
    * **Use query**: Select the Query option.
    * **Query**: Enter the following SQL query to get the names of the tables in the OLTP schema.

              SELECT SCHEMA_NAME(t.schema_id) AS schema_name,
              t.name as table_name
              FROM sys.tables t
              WHERE SCHEMA_NAME(t.schema_id) = 'OLTP' 
              ORDER BY table_name


    * **First row only**: Make sure this is **unchecked** (you need to get all tables, not just the first one).
    * **Timeout**: Leave the default value or adjust it if your queries take a long time.

#### Activity: ForEach (Iterate over OLTP Tables)
This activity iterates over the output of the Lookup activity (the list of OLTP tables) to process each table individually.
* Click on the **ForEach activity** (likely named something like ForEach OLTP Table). 🔁
* **General** tab: 
  * Verify the activity's **Name** (e.g., ForEach OLTP Table). 
  * Ensure it is Enabled. 
* **Settings** tab: 
  * **Sequential**: Check this box ✅ if you want the iterations (the copy of each table) to run one after another. Uncheck it if you want them to run in parallel (can be faster but uses more resources). For this simulation, processing sequentially might be easier to debug. \
  * **Items**: Click in the field and select **Add dynamic content**. Here you must select the **output of the Lookup activity** that provides the list of tables. The dynamic expression should look something like
    
           @activity('YourLookupActivityName').output.value.
    
    Make sure to replace **'YourLookupActivityName'** with the exact name you gave your Lookup activity in the pipeline. This tells the ForEach to iterate over each object (each table) in the list of results from the Lookup query.
* Inside the **ForEach activity**, you will see the **Copy Data** activity that will run for each table. Double-click on the **ForEach** to enter and configure the internal activities.

#### Activity: Copy Data (Inside ForEach - OLTP to Bronze)
This activity runs for each OLTP table found by the Lookup and copies the data from that table to the Bronze layer of the Data Lakehouse.
* Click on the **Copy Data** activity inside the ForEach (likely named something like Copy OLTP to Bronze). ➡️📦 
* General tab:
  * Verify the activity's **Name** (e.g., Copy OLTP to Bronze).
  * Ensure it is **Enabled**.
* **Source** tab:
  * **Connection**: Select the connection to your local SQL Server that you created previously (e.g., OnPremSQLGateway). **This name must exactly match the name of your connection in Fabric**.
  * **Connection type**: Select SQL server.
  * **Database**: Select or type the name of your database: RetailNova.
  * **Use query**: Select the Table option.
  * **Table**: Click in the field and select **Add dynamic content**. Here you will use the ForEach variables that contain the current table's schema and name. It should look something like:

            @item().schema_name for the schema and @item().table_name

    for the table name. This makes the activity dynamically read the current table in the ForEach iteration. Make sure to check the "Enter manually" box if needed to enter the dynamic expressions.
  * **Advanced**: Review advanced options if you need specific configuration (e.g., Isolation level, Command timeout).
* **Destination** tab:
  * **Connection**: Select your destination Lakehouse: LH_Bronze.
  * **Root folder**: Select the Tables option. This indicates that the data will be written to the managed tables section of the Lakehouse, creating Delta tables.
  * **Table**: Click in the field and select Add dynamic content. Similar to the source, you will use the ForEach variable to dynamically name the destination Delta table:
    
           @item().table_name
    This will create or append data to a Delta table in Bronze with the same name as the source OLTP table.
  * **Table action**: Select the Overwrite option. This will overwrite the destination Delta table with the data from the current batch in each execution for this table.
  * **Enable partitions**: Make sure this is unchecked.

#### Activity: Notebook (Bronze to Silver)
This activity executes the Notebook that processes data from Bronze to Silver. The detailed configuration of this Notebook is found in the next section.
* Click on the **Notebook activity** (Bronze to Silver). 📦✨
* **General** tab:
  * Verify the activity's **Name** (Bronze to Silver).
  * Ensure it is **Enabled**.
* **Settings** tab:
  * **Workspace**: Select the Workspace where your Notebook is located (e.g., RetailNova_Batch).
  * **Notebook**: Select the specific Notebook that performs the Bronze to Silver transformation (ETL_Bronze_to_Silver).
  * **Advanced settings**: Review advanced options if you need specific configuration (e.g., Spark configuration, Environment).
#### Activity: Notebook (Silver to Gold)
This activity executes the Notebook that processes data from Silver to Gold. The detailed configuration of this Notebook is found in the next section.
* Click on the Notebook activity (Silver to Gold). ✨🥇
* **General** tab:
  * Verify the activity's **Name** (Silver to Gold).
  * Ensure it is **Enabled**.
* **Settings** tab:
  * **Workspace**: Select the Workspace where your Notebook is located (e.g., RetailNova_Batch).
  * **Notebook**: Select the specific Notebook that performs the Silver to Gold transformation (ETL_Silver_to_Gold).
  * **Advanced settings**: Review advanced options if you need specific configuration (e.g., Spark configuration, Environment).
#### Activity: ForEach_m4l (Load from Gold to Warehouse)
This activity iterates over a list of items (defined by a pipeline parameter) and executes a Copy Data activity for each item. In this pipeline, it is used to copy data from the Gold layer to the Warehouse tables.
* Click on the ForEach activity (ForEach_m4l). 🔁📈
* General tab:
  * Verify the activity's Name (ForEach_m4l).
  * Ensure it is Enabled.
* Settings tab:
  * **Sequential**: Make sure this is unchecked.
  * **Batch count**: Leave blank or adjust if you need to control the number of parallel iterations.
  * **Items**: Click in the field and select Add dynamic content. The dynamic expression should be
   
           @pipeline().parameters.cw_items_m4l
    This indicates that the ForEach will iterate over the items provided by the pipeline parameter

           cw_items_m4l
    This parameter likely contains a list of table names or paths to copy from Gold to the Warehouse.
* Inside the ForEach_m4l activity, you will see the Copy Data activity that will run for each item. Double-click on the ForEach to enter and configure the internal activity.
  
#### Activity: Copy_m4l (Inside ForEach_m4l - Gold to Warehouse)
This activity runs for each item provided by the ForEach_m4l and copies the data from the Gold layer to the Warehouse layer.
* Click on the **Copy Data** activity inside the ForEach_m4l (Copy_m4l). ➡️📈
* **General** tab:
  * Verify the activity's **Name** (Copy_m4l).
  * Ensure it is **Enabled**.
* Source tab:
  * **Connection**: Select your source Lakehouse: LH_Gold.
  * **Root folder**: Select the Tables option.
  * **Table**: Click in the field and select Add dynamic content. The dynamic expression must be
  
           @item().source.table
    This indicates that the activity will read the table specified in the **source.table** property of the current item the **ForEach_m4l** is iterating over.
* **Destination** tab:
  * **Connection**: Select your destination Warehouse: WH_Report.
  * **Table option**: Select Auto create table. This indicates that Fabric will automatically create the destination table in the Warehouse if it doesn't exist, based on the schema of the source data.
  * **Table**: In the first field, type the destination schema, which is **dbo** according to the screenshot. In the second field, click and select **Add dynamic content**. The dynamic expression must be

           @item().destination.table
    This indicates that the destination table name in the Warehouse will be taken from the destination.table property of the current item the **ForEach_m4l** is iterating over.
  * **Advanced**: In the advanced options, in the **"Pre-copy script"** field, enter the following script:
  
              TRUNCATE TABLE @{item().destination.table};

This script will execute in the destination Warehouse **before** the data copy starts for the current table in the ForEach iteration. It ensures that the destination table is completely emptied before loading the new data from Gold.Review other advanced options if you need specific configuration (e.g., Write behavior Insert, Upsert, Overwrite). The combination of "Auto create table" and "Pre-copy script" with TRUNCATE achieves a full "overwrite" effect for the table in the Warehouse with the data from Gold in each execution for that table.
* **Mapping** tab:
  * **Mapping**: This field has the dynamic expression
    
           @item().copyActivity.translator
    This indicates that the column mapping between the source (table in LH_Gold) and the destination (table in WH_Report) is dynamically defined using the copyActivity.translator property of the current item the ForEach_m4l is iterating over. This is useful if you need custom mappings for each table being copied within the ForEach.
After configuring all pipeline activities and enabling them, save the pipeline. It is now ready to be executed.

### 📓 Notebook Configuration
The "Bronze to Silver" and "Silver to Gold" Notebooks are key components of the pipeline that perform the transformations and incremental loading. Their main configuration is done within the Notebook itself, but it's important to understand how they connect to the Lakehouses and manage watermarks.
#### Notebook: ETL_Bronze_to_Silver
* **Purpose**: Read incremental data from the Bronze layer (LH_Bronze), apply transformations, and write to the Silver layer (LH_Silver).
* **Location**: This Notebook must exist in your Fabric Workspace (e.g., RetailNova_Batch).
* **Specific Configuration**:
  * **Workspace Name**: Inside the Notebook's PySpark code, there is a variable that defines the workspace name. **You must change this variable to exactly match the name of your Microsoft Fabric workspace where you imported the repository content.** Look for the line similar to:
  
         workspace_name = "RetailNova_Batchv2" # **ADJUST THIS TO YOUR ACTUAL WORKSPACE NAME**

Modify "RetailNova_Batchv2" with your workspace name.
* **Connections (Attached Lakehouses)**: For the Notebook to be able to read from and write to the Lakehouses, you must ensure that LH_Bronze and LH_Silver are **attached** to this Notebook in its configuration. This is done in the Notebook's user interface, in the Lakehouses section. The Lakehouse names (LH_Bronze, LH_Silver) are already defined in the Notebook code and should match the ones you created if you followed the previous steps.
* **Data Paths**: Within the Notebook's PySpark code, ABFS (Azure Blob File System) paths are used to reference the Delta tables in the Lakehouses. These paths have the format

       abfss://<workspace_name>@onelake.dfs.fabric.microsoft.com/<lakehouse_name>.Lakehouse/Tables/<table_name>
  The Notebook uses the workspace_name variable and the Lakehouse names to build these paths dynamically. That's why it's crucial to only adjust the workspace name.
* **High-Watermark**: The Notebook manages a high-watermark file (bronze_orders_high_watermark) to track the last processed order date. This file is saved in the Files/HighWatermark/ section of the source Lakehouse (LH_Bronze). The Notebook reads this file at the beginning to know where to start processing from and updates it at the end with the maximum date of the processed data.
* **Parameters**: If the Notebook required external inputs (like a specific table name or start date), parameters would be defined in its cells (using %param) and assigned static or dynamic values in the pipeline's Notebook activity. In this project, the Notebook reads paths internally, so it typically doesn't require input parameters.
#### Notebook: ETL_Silver_to_Gold
* **Purpose**: Transform the clean data from Silver into a dimensional model optimized for BI (fact and dimension tables).
* **Location**: This Notebook must exist in your Fabric Workspace (e.g., RetailNova_Batch).
* **Specific Configuration**:
  * Workspace Name: Similar to the previous Notebook, you must adjust the workspace_name variable within the PySpark code to match your Microsoft Fabric workspace name. Look for the line similar to:
  
          workspace_name = "RetailNova_Batchv2" # **ADJUST THIS TO YOUR ACTUAL WORKSPACE NAME**

Modify "RetailNova_Batchv2" with your workspace name.
  * **Connections (Attached Lakehouses)**: You must ensure that LH_Silver and LH_Gold are attached to this Notebook in its configuration. The Lakehouse names (LH_Silver, LH_Gold) are already defined in the Notebook code.
  * **Data Paths**: Similar to the previous Notebook, it uses the workspace_name variable and the Lakehouse names to build the ABFS paths to read from LH_Silver and write to LH_Gold.
  * **High-Watermark**: This Notebook manages its own high-watermark (silver_sales_high_watermark) to track the processing timestamp of the data in Silver. This file is saved in the Files/HighWatermark/ section of the destination Lakehouse (LH_Gold).
  * **Dimensional Logic**: Within the PySpark code, this Notebook performs the necessary joins to build the FactSales fact table and manages the incremental loading of dimension tables (like DimCategory) using techniques like MERGE or INSERT NOT EXISTS).
  * **Parameters**: Configure parameters if this Notebook uses them.

### ▶️ How to Run the Pipeline
Follow these steps to run the complete flow:
1. **Generate Data in Local SQL Server:**
  * Open a terminal or Command Prompt on your local machine, or use a Python code interpreter like Visual Studio Code, PyCharm, etc.
  * Navigate to the folder where you saved generate_and_ingest_local_sql.py and your .env file.
  * Run the script:

              python generate_and_ingest_local_sql.py

  * Observe the script's output to confirm that it connected to SQL Server (reading credentials from .env), generated data, created/cleaned OLTP tables, and populated the OLTP tables without errors.
  * You can run this script multiple times to simulate the arrival of new data batches.
2. **Run the Pipeline in Microsoft Fabric:**
  * Go to your Workspace in Microsoft Fabric.
  * Find the data pipeline you configured.
  * Click "Run" to start the pipeline execution.
The pipeline will connect to your local SQL Server (via the gateway configured in Fabric), copy the data to Bronze, and then the Notebooks will incrementally process the new data to Silver and Gold. Finally, the *ForEach_m4l* with *Copy_m4l* will copy the specified tables from Gold to the Warehouse.
### ⏳ Incremental Loading and High-Watermarks
* The pipeline is designed for incremental loading.
* The Python script generates timestamps for orders using the current UTC time, ensuring that new data is always later than previous watermarks in Fabric.
* The "Bronze to Silver" and "Silver to Gold" Notebooks manage high-watermark files (bronze_orders_high_watermark and silver_sales_high_watermark) in the Files/HighWatermark folder of their respective source Lakehouses (primarily in LH_Bronze and LH_Gold). These files record the timestamp of the last processed data, allowing subsequent runs to process only the most recent data.
* The load from Gold to Warehouse via the *ForEach_m4l/Copy_m4l* implements a **full overwrite** strategy for the destination tables in the Warehouse. This is achieved by using the **"Pre-copy script"** configured in the **"Destination"** tab of the Copy Data activity. This script executes

         TRUNCATE TABLE @{item().destination.table};
  before each data copy operation, deleting all existing data in the destination Warehouse table before loading the current data from the Gold layer. Additionally, the **"Auto create table"** option is enabled, which allows the destination table to be automatically created in the Warehouse if it doesn't exist, based on the schema of the source table in Gold. This approach ensures that the tables in the Warehouse always reflect the most recent state of the corresponding tables in the Gold layer.

### 📊 Integration with the Semantic Model (Analysis Layer)
A fundamental aspect of this project is how the processed data in the Warehouse (WH_Report) is made available for analysis and reporting. This is achieved through a **Semantic Model** (formerly known as a Dataset in Power BI). In this project, the Semantic Model is named **"RetailNova_Report"**.
* **Direct Connection**: The "RetailNova_Report" Semantic Model is created in Fabric and connects directly to the Warehouse. There is no need to import or duplicate data; the Semantic Model queries the tables (such as FactSales, DimCategory, etc.) that reside in the Warehouse.
* **Model Refresh**: Each time the pipeline runs and loads new data into the Warehouse tables, the **"RetailNova_Report"** Semantic Model **does not automatically update**. For reports and dashboards using this Semantic Model to show the most recent data, a **"Refresh"** operation is required on the Semantic Model.
* **Automating Refresh**: In a production scenario, the refresh of the **"RetailNova_Report"** Semantic Model should be automated. This can be configured in the Semantic Model settings in Fabric, scheduling it to run periodically (for example, after each successful pipeline execution) or triggering it via APIs or orchestration tools. **It is important to note that since this project implements a batch processing pipeline, updating the Semantic Model via a scheduled or triggered refresh is the appropriate and consistent approach with the architecture. Technologies like Direct Lake in Fabric allow for a more direct connection to data in the Lakehouse without requiring explicit refreshes to achieve near real-time latency, but that is not the goal or design pattern of this batch pipeline**.
* **Importance for BI**: This connection and the refresh process are crucial. They allow Business Intelligence tools (like Power BI) to build reports and visualizations on a clean and updated dimensional data model, without BI users needing to interact directly with the complex structures of the Lakehouse or the ETL process.
This point highlights how the pipeline not only moves and transforms data but also prepares and presents the information optimally for final consumption in the analysis layer.

### 🐞 Common Troubleshooting
* **Restarting the Entire Process**: If you encounter persistent issues or want to start from a clean state, you can delete all tables generated by the pipeline in the Lakehouses (LH_Bronze, LH_Silver, LH_Gold) and delete the watermark folders or files (HighWatermark) in the Files section of the relevant Lakehouses (primarily in LH_Bronze and LH_Gold). Once done, you can re-run the generate_and_ingest_local_sql.py script to generate a new initial batch of data in your local SQL Server and then run the Full_Process pipeline in Fabric to process this new data from scratch.
* **SQL Server Connection Errors (from Python script)**: Verify that the .env file is in the same folder as the script, that the variable names (SQL_SERVER_NAME, etc.) and their values are correct, and that the SQL_DRIVER name exactly matches an installed driver. Ensure the SQL Server service is running and the user has permissions.
* **Data Gateway Errors (from Fabric)**: Ensure the On-premises Data Gateway is installed, configured, and running on the correct machine. Verify the connection configuration in Fabric, including the SQL Server credentials stored in the connection.
* **Notebook not detecting new data**:
  * Ensure you are using the latest version of the local Python script that generates timestamps in UTC.
  * Check the OrderDates in the OLTP.Orders table in your local SQL Server after running the Python script. They should be recent timestamps and later than the watermark recorded in the "Bronze to Silver" Notebook's log (in UTC).
  * Verify that the Copy Data activity is correctly copying data to the Bronze layer.
* **Invalid column name errors in Notebooks**: This indicates a mismatch between the expected column names in the Notebook and the actual column names in the source tables (Bronze or Silver). Check the schemas of the tables in your Lakehouses and compare them with the Notebook code.
* **Unexpected NULLs in the Gold layer**: If you see NULLs in surrogate keys or metrics in FactSales, review the "Silver to Gold" Notebook, especially the joins with dimension tables and metric calculations. Ensure joins are performed correctly and that source columns are not NULL before calculations.
* **Data not appearing in the Warehouse**: Check the configuration of the ForEach_m4l and the Copy_m4l activity within it. Ensure the cw_items_m4l pipeline parameter contains the correct names of the Gold tables to copy. Review the source configuration (pointing to LH_Gold.Tables) and destination (pointing to your WH_Report.Tables). Check the "Table action" (Append/Overwrite) and ensure it is the desired one.
* **Updated data in Warehouse not visible in reports/dashboards**: Make sure you have performed a Refresh on the "RetailNova_Report" Semantic Model that feeds those reports. If the refresh fails, check the refresh history in Fabric to see error details, which often point to connection issues to the Warehouse or errors within the model itself (e.g., broken relationships).
## 👋 Contributions
If you wish to contribute to this project, please follow the standard GitHub steps: fork the repository, create a branch for your changes, make your modifications, and submit a pull request.
