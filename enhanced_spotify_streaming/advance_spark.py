from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

def create_spark_session():
    return SparkSession \
        .builder \
        .appName("AdvancedSpotifyAnalytics") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()

def process_stream(df, epoch_id):
    """Process stream with advanced analytics"""
    try:
        # Skip processing if dataframe is empty
        if df.rdd.isEmpty():
            print("No data received in this batch...")
            return

        # 1. Basic Statistics
        print("\n=== Basic Statistics ===")
        df.select("artist", "popularity", "danceability", "energy") \
          .groupBy("artist") \
          .agg(
              avg("popularity").alias("avg_popularity"),
              avg("danceability").alias("avg_danceability"),
              avg("energy").alias("avg_energy"),
              count("*").alias("track_count")
          ) \
          .orderBy(desc("track_count")) \
          .show(truncate=False)

        # 2. Feature Analysis
        if df.count() > 1:  # Only perform clustering if we have enough data
            # Prepare features for clustering
            feature_cols = ["danceability", "energy", "tempo"]
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            feature_vectors = assembler.transform(df)

            # Perform clustering
            kmeans = KMeans(k=3, featuresCol="features", predictionCol="cluster")
            model = kmeans.fit(feature_vectors)
            clustered_data = model.transform(feature_vectors)

            # Analyze clusters
            print("\n=== Song Clusters Analysis ===")
            clustered_data.groupBy("cluster") \
                .agg(
                    count("*").alias("songs_in_cluster"),
                    round(avg("popularity"), 2).alias("avg_popularity"),
                    round(avg("danceability"), 2).alias("avg_danceability"),
                    round(avg("energy"), 2).alias("avg_energy"),
                    collect_list("name").alias("sample_songs")
                ) \
                .orderBy("cluster") \
                .show(truncate=False)

        # 3. Trending Analysis
        print("\n=== Trending Songs ===")
        df.select("name", "artist", "popularity") \
          .orderBy(desc("popularity")) \
          .show(5, truncate=False)

    except Exception as e:
        print(f"Error in stream processing: {e}")

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for Spotify data
    schema = StructType([
        StructField("name", StringType()),
        StructField("artist", StringType()),
        StructField("popularity", DoubleType()),
        StructField("danceability", DoubleType()),
        StructField("energy", DoubleType()),
        StructField("tempo", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
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
    
    # Process streaming data
    query = parsed_df \
        .writeStream \
        .foreachBatch(process_stream) \
        .trigger(processingTime='10 seconds') \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
