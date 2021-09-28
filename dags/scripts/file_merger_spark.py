import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, input_file_name, lit, regexp_extract, element_at, split
from pyspark.sql.types import LongType
import file_deleter as fd

# Set uo our sparkj session
spark = SparkSession.builder \
                    .master("local") \
                    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
                    .getOrCreate()


# Declare some paths
input_dir_json = '/Users/oliver.bradley/airflow/input_source_1/'
input_dir_csv =  '/Users/oliver.bradley/airflow/input_source_2/'
output_dir = "/Users/oliver.bradley/airflow/output_dest/"

# Get all json files in our source folder and extract the date from each file name
df_json = spark.read.json(f'''{input_dir_json}*.json''') \
               .withColumn("date", regexp_extract(element_at(split(input_file_name(), "/"),-1), "\\d+", 0)) \
               
#Do the same with our csv's
df_csv = spark.read.option("header", True).csv(f'''{input_dir_csv}*.csv''') \
              .withColumn("date", regexp_extract(element_at(split(input_file_name(), "/"),-1), "\\d+", 0))

df_merged = df_json.alias("json").join(df_csv.alias("csv"), ["post_id", "date"], "full_outer") \
                   .withColumn("shares", coalesce(col("json.shares.count"), lit(0)).cast(LongType())) \
                   .withColumn("comments", coalesce(col("comments"), lit(0)).cast(LongType())) \
                   .withColumn("likes", coalesce(col("likes"), lit(0)).cast(LongType())) \
                   .select("post_id", "shares", "comments", "likes", "date") \

#Write out our CSV's 
df_merged.coalesce(1) \
.write \
.format("csv") \
.option("header", "true") \
.partitionBy("date") \
.mode("Overwrite") \
.save(f'''{output_dir}''')

# Clean up metadata files left over from spark write
fd.remove_files_recursively(output_dir, ('.crc'))


