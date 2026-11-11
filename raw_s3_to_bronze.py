from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Bronze Delta").getOrCreate()

raw_s3_path = "s3://databrick-mysql-bucket/test1/employee/"
bronze_s3_path = "s3://databrick-mysql-bucket/bronze"  
bronze_checkpoint_path = "s3://databrick-mysql-bucket/_checkpoints/bronze_employee/"
bronze_table_name = "main.default.bronze_mysql_table"
bronze_schema_path = "s3://databrick-mysql-bucket/_schemas/bronze_employee"

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("recursiveFileLookup", "true")
    .option("cloudFiles.includeExistingFiles", "true") 
    .option("cloudFiles.schemaLocation", bronze_schema_path)                 
    .load(raw_s3_path)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", bronze_s3_path)  
    .option("mergeSchema", "true")                
    .option("checkpointLocation", bronze_checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table_name)
)

print(f"Streaming job started for Bronze table: {bronze_table_name} with checkpoint at: {bronze_checkpoint_path}")