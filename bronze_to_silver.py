from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number, upper, trim, coalesce, lit, nullif
from pyspark.sql.window import Window

silver_table_name = "main.default.silver_employee"
silver_s3_path = "s3://databrick-mysql-bucket/silver"

# 1. Enforce strict schema constraints (Disable auto-merge at session level)
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

# 2. Determine the high-watermark timestamp
if spark.catalog.tableExists(silver_table_name):
    last_processed_time = spark.sql(f"SELECT max(dms_update_time) FROM {silver_table_name}").collect()[0][0]
    if not last_processed_time:
        last_processed_time = "1970-01-01 00:00:00"
else:
    last_processed_time = "1970-01-01 00:00:00"

# 3. Read new records from Bronze and handle Column Renaming (ID -> emp_id)
# COALESCE ensures that if the source uses old 'ID' or new 'emp_id', it bridges cleanly.
bronze_df = spark.sql(f"""
    SELECT *, 
           UPPER(TRIM(Op)) as raw_op,
           COALESCE(employee_id, ID) as emp_id 
    FROM main.default.bronze_mysql_table 
    WHERE dms_update_time > '{last_processed_time}'
""")

# Map NULL or empty string Op values to 'I' (Insert/Baseline data)
bronze_processed_df = bronze_df.withColumn(
    "clean_op", 
    coalesce(nullif(col("raw_op"), lit("")), lit("I"))
)

if not spark.catalog.tableExists(silver_table_name):
    # Day-Zero Initial Load: Filter out deletes if any snuck in
    initial_df = bronze_processed_df.filter(col("clean_op") != "D")
    
    window_spec = Window.partitionBy("employee_id").orderBy(col("dms_update_time").desc())
    initial_dedup_df = (
        initial_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn", "raw_op", "clean_op", "ID") # Drop old redundant ID column if present
    )
    
    (initial_dedup_df.write
     .format("delta")
     .mode("overwrite")
     .option("path", silver_s3_path)
     .saveAsTable(silver_table_name)
    )
    print(f"Successfully initialized External Silver table with schema enforcement at: {silver_s3_path}")

else:
    if bronze_processed_df.isEmpty():
        print("No new data in Bronze. Skipping Silver processing.")
    else:
        # Deduplicate incoming micro-batch (keep latest record per emp_id)
        window_spec = Window.partitionBy("employee_id").orderBy(col("dms_update_time").desc())
        latest_incoming_df = (
            bronze_processed_df.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn", "raw_op", "ID")
        )

        # 4. Strict Schema-Enforced Delta MERGE
        silver_table = DeltaTable.forName(spark, silver_table_name)
        
        (silver_table.alias("target")
         .merge(
             latest_incoming_df.alias("source"),
             "target.employee_id = source.employee_id"
         )
         # Notice: No .withSchemaEvolution() here. Any unexpected column change will fail fast 
         # or be dropped depending on strict mode settings.
         .whenMatchedDelete(condition = col("source.clean_op") == "D")
         .whenMatchedUpdateAll(condition = col("source.clean_op").isin("U", "I"))
         .whenNotMatchedInsertAll(condition = col("source.clean_op") == "I")
         .execute()
        )
        print(f"Successfully processed schema-enforced batch into: {silver_table_name}")