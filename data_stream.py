import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField("crime_id", StringType(), True), 
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True), 
    StructField("original_crime_type_name", StringType(), True), 
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True), 
    StructField("city", StringType(), True), 
    StructField("disposition", StringType(), True), 
    StructField("address", StringType(), True), 
    StructField("state", StringType(), True), 
    StructField("agency_id", StringType(), True), 
    StructField("address_type", StringType(), True), 
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

   df = spark \
         .readStream \
         .format("kafka") \
         .option("kafka.boostrap.servers", "localhost:9092") \
         .option("startingOffsets", "earliest") \
         .option("subscribe", "service-calls") \
         .option("maxOffsetsPerTrigger", 200) \
         .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    
    service_table = kafka_df.select(psf.from_json(psf.col('value'), schema).alias("DF")).select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table.select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition')) 

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name", psf.window("call_date_time", "60 minutes")).count()
    
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    
    #write output stream
    query = agg_df.writeStream.queryName("agg_query_writer").outputMode("Complete").format("console").start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)
    
    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = agg_df.join(radio_code_df, "disposition" )
    
    join_query_writer = join_query\
            .writeStream\
            .queryName("join_query_writer")\
            .outputMode("append")\
            .format("console")\
            .start()


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()