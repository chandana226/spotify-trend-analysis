from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import mmh3
import random
from collections import deque, defaultdict
import math

class FlajoletMartin:
    def __init__(self, num_estimators=32):
        self.num_estimators = num_estimators
        self.max_zeros = [0] * num_estimators
        
    def add(self, item):
        """Add an item to the FM sketch"""
        for i in range(self.num_estimators):
            hash_val = mmh3.hash(str(item), seed=i)
            trailing_zeros = len(bin(hash_val)[2:].rstrip('0'))
            self.max_zeros[i] = max(self.max_zeros[i], trailing_zeros)
    
    def estimate(self):
        """Estimate the cardinality"""
        return 2 ** (sum(self.max_zeros) / self.num_estimators)

class DGIM:
    def __init__(self, window_size):
        self.window_size = window_size
        self.buckets = deque()
        self.timestamp = 0
        
    def add(self, bit):
        """Add a new bit to the window"""
        if bit == 1:
            self.buckets.append((self.timestamp, 1))
            self._merge_buckets()
        
        # Remove old buckets
        while self.buckets and self.timestamp - self.buckets[0][0] >= self.window_size:
            self.buckets.popleft()
            
        self.timestamp += 1
    
    def _merge_buckets(self):
        """Merge buckets of the same size"""
        i = len(self.buckets) - 1
        while i > 0:
            if len(self.buckets) < 2:
                break
            if self.buckets[i][1] == self.buckets[i-1][1]:
                ts = min(self.buckets[i][0], self.buckets[i-1][0])
                size = self.buckets[i][1] * 2
                self.buckets.pop()
                self.buckets.pop()
                self.buckets.append((ts, size))
                i -= 2
            else:
                i -= 1
    
    def count_ones(self):
        """Count number of ones in the current window"""
        return sum(bucket[1] for bucket in self.buckets)

class SimpleXAIModule:
    def __init__(self):
        self.feature_names = ['popularity', 'danceability', 'energy', 'tempo']
    
    def feature_importance(self, data):
        """Calculate simple feature importance using correlations"""
        importance_scores = defaultdict(float)
        for feature in self.feature_names:
            if feature in data.columns:
                correlation = data[feature].corr(data['popularity'])
                importance_scores[feature] = abs(correlation) if not np.isnan(correlation) else 0.0
        return importance_scores

class FairnessMetrics:
    def __init__(self):
        self.protected_attributes = ['artist']
        
    def representation_balance(self, data, attribute='artist'):
        """Calculate representation balance across artists"""
        if len(data) == 0:
            return 0.0
            
        total_count = len(data)
        group_counts = data[attribute].value_counts()
        
        entropy = 0
        for count in group_counts:
            if count > 0:
                prob = count/total_count
                entropy -= prob * (math.log(prob) if prob > 0 else 0)
        return entropy

class MachineUnlearning:
    def __init__(self):
        self.forgotten_items = set()
        self.feature_masks = defaultdict(lambda: True)
        
    def forget_item(self, item_id):
        """Mark an item for forgetting"""
        self.forgotten_items.add(item_id)
        
    def forget_feature(self, feature_name):
        """Mask a feature for selective forgetting"""
        self.feature_masks[feature_name] = False
        
    def apply_unlearning(self, df):
        """Apply unlearning transformations to data"""
        # Remove forgotten items
        filtered_df = df.filter(~col("name").isin(list(self.forgotten_items)))
        
        # Apply feature masks
        for feature, included in self.feature_masks.items():
            if not included and feature in df.columns:
                filtered_df = filtered_df.withColumn(feature, lit(None))
                
        return filtered_df

class EnhancedSpotifyAnalytics:
    def __init__(self, spark):
        self.spark = spark
        # Initialize components
        self.fm_sketch = FlajoletMartin()
        self.dgim_tracker = DGIM(window_size=100)
        self.xai_module = SimpleXAIModule()
        self.fairness_metrics = FairnessMetrics()
        self.unlearning = MachineUnlearning()
        
    def process_batch(self, df, epoch_id):
        try:
            if df.count() == 0:
                print("No data in this batch")
                return

            print("\n==== Processing Batch ====")
            
            # Convert to pandas for some calculations
            pdf = df.toPandas()
            
            # 1. Cardinality Estimation (Flajolet-Martin)
            print("\n=== Streaming Algorithms Results ===")
            for row in df.collect():
                self.fm_sketch.add(row['name'])
            print(f"Estimated unique songs: {self.fm_sketch.estimate():.0f}")
            
            # 2. Feature Importance (Simple XAI)
            print("\n=== Feature Importance Analysis ===")
            importance_scores = self.xai_module.feature_importance(pdf)
            print("Feature Importance:")
            for feature, score in importance_scores.items():
                print(f"- {feature}: {score:.3f}")
            
            # 3. Fairness Metrics
            print("\n=== Fairness Metrics ===")
            entropy = self.fairness_metrics.representation_balance(pdf)
            print(f"Artist representation balance (entropy): {entropy:.3f}")
            
            # 4. Unlearning Status
            print("\n=== Unlearning Status ===")
            forgotten_count = len(self.unlearning.forgotten_items)
            print(f"Number of forgotten items: {forgotten_count}")
            print("Masked features:", [f for f, m in self.unlearning.feature_masks.items() if not m])
            
            # 5. Basic Statistics
            print("\n=== Basic Statistics ===")
            df.groupBy("artist") \
              .agg(
                  count("*").alias("track_count"),
                  round(avg("popularity"), 2).alias("avg_popularity"),
                  round(avg("danceability"), 2).alias("avg_danceability")
              ) \
              .orderBy(desc("track_count")) \
              .show(5, truncate=False)
              
        except Exception as e:
            print(f"Error processing batch: {e}")

def create_spark_session():
    return SparkSession \
        .builder \
        .appName("EnhancedSpotifyAnalytics") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .master("local[*]") \
        .getOrCreate()

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Starting Enhanced Spotify Analytics...")
    
    # Initialize analytics
    analytics = EnhancedSpotifyAnalytics(spark)
    
    # Define schema
    schema = StructType([
        StructField("name", StringType()),
        StructField("artist", StringType()),
        StructField("popularity", DoubleType()),
        StructField("danceability", DoubleType()),
        StructField("energy", DoubleType()),
        StructField("tempo", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    try:
        # Read stream
        streaming_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "spotify_stream") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON
        parsed_df = streaming_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Process stream
        query = parsed_df \
            .writeStream \
            .foreachBatch(analytics.process_batch) \
            .trigger(processingTime='10 seconds') \
            .start()
            
        print("Streaming started. Waiting for data...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in main: {e}")
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
