from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """Create Spark session with fixed local configuration"""
    return SparkSession \
        .builder \
        .appName("EnhancedSpotifyStreamingConsumer") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .master("local[*]") \
        .getOrCreate()

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Starting Enhanced Spotify streaming consumer...")
    
    # Define schema for the streaming data
    schema = StructType([
        StructField("name", StringType()),
        StructField("artist", StringType()),
        StructField("popularity", IntegerType()),
        StructField("danceability", DoubleType()),
        StructField("energy", DoubleType()),
        StructField("tempo", DoubleType()),
        StructField("timestamp", StringType())
    ])
    
    try:
        # Read from Kafka
        streaming_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "spotify_stream") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = streaming_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Process the stream
        stats = parsed_df \
            .groupBy("artist") \
            .agg(
                avg("popularity").alias("avg_popularity"),
                avg("danceability").alias("avg_danceability"),
                avg("energy").alias("avg_energy"),
                count("*").alias("track_count")
            ) \
            .orderBy(desc("track_count"))
        
        # Write output to console
        query = stats \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        print("Consumer started successfully! Waiting for data...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming: {e}")
        spark.stop()

if __name__ == "__main__":
    main()