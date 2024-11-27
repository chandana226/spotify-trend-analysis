from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import mmh3
import random
from datasketch import MinHash, MinHashLSH

class SpotifyAnalytics:
    def __init__(self, spark):
        """Initialize analytics components"""
        self.spark = spark
        # Initialize streaming algorithms
        self.bloom_filter = BloomFilter(size=1000, num_hash_functions=3)
        self.reservoir = ReservoirSampling(k=10)
        # Initialize privacy components
        self.dp = DifferentialPrivacy(epsilon=0.5)
        self.k_anon = KAnonymity(k=2)
        # Initialize LSH
        self.lsh = LSHSimilarity(threshold=0.5, num_perm=128)
        
    def process_batch(self, df, epoch_id):
        """Process each batch of streaming data"""
        try:
            if df.isEmpty():
                print("No data in this batch")
                return

            print("\n==== Processing Batch ====")
            
            # 1. Streaming Algorithms
            print("\n=== Streaming Algorithms Results ===")
            self.apply_streaming_algorithms(df)
            
            # 2. Privacy Techniques
            print("\n=== Privacy-Preserved Analytics ===")
            self.apply_privacy_techniques(df)
            
            # 3. Similarity Analysis
            print("\n=== Song Similarity Analysis ===")
            self.analyze_similarities(df)
            
            # 4. Basic Statistics
            print("\n=== Basic Statistics ===")
            self.show_basic_stats(df)
            
        except Exception as e:
            print(f"Error processing batch: {e}")
    
    def apply_streaming_algorithms(self, df):
        """Apply streaming algorithms to the batch"""
        try:
            rows = df.collect()
            
            # Apply Bloom Filter for popular songs
            for row in rows:
                if row['popularity'] and float(row['popularity']) > 80:
                    self.bloom_filter.add(row['name'])
                    print(f"Added popular song to Bloom Filter: {row['name']}")
            
            # Apply Reservoir Sampling
            for row in rows:
                self.reservoir.add({
                    'name': row['name'],
                    'artist': row['artist'],
                    'popularity': row['popularity']
                })
            
            print("\nRandom sample of songs (Reservoir Sampling):")
            sample = self.reservoir.get_sample()
            for song in sample[:3]:  # Show first 3 samples
                print(f"- {song['name']} by {song['artist']} (Popularity: {song['popularity']})")
                
        except Exception as e:
            print(f"Error in streaming algorithms: {e}")

    def apply_privacy_techniques(self, df):
        """Apply privacy preservation techniques"""
        try:
            # Calculate stats safely
            stats_df = df.agg(
                avg("popularity").alias("avg_popularity"),
                avg("danceability").alias("avg_danceability"),
                avg("energy").alias("avg_energy")
            ).collect()
            
            if stats_df and len(stats_df) > 0:
                row = stats_df[0]
                # Add differential privacy noise to statistics
                private_stats = {
                    'popularity': self.dp.add_noise(float(row['avg_popularity'] or 0)),
                    'danceability': self.dp.add_noise(float(row['avg_danceability'] or 0)),
                    'energy': self.dp.add_noise(float(row['avg_energy'] or 0))
                }
                
                print("\nPrivacy-preserved statistics:")
                for key, value in private_stats.items():
                    print(f"Average {key}: {value:.2f}")
            
            # K-anonymity
            # Convert DataFrame to list of dictionaries for K-anonymity
            data = [{
                'popularity': float(row['popularity']) if row['popularity'] is not None else None,
                'danceability': float(row['danceability']) if row['danceability'] is not None else None,
                'energy': float(row['energy']) if row['energy'] is not None else None
            } for row in df.collect()]
            
            if data:
                anonymized = self.k_anon.anonymize(data)
                
                print("\nK-anonymized data sample (first 3 groups):")
                for i, record in enumerate(anonymized[:3]):
                    print(f"Group {i+1}:", record)
                    
        except Exception as e:
            print(f"Error in privacy techniques: {e}")
    
    def analyze_similarities(self, df):
        """Analyze song similarities using LSH"""
        try:
            rows = df.collect()
            if not rows:
                return
                
            # Add songs to LSH
            for row in rows:
                features = self._extract_features(row)
                self.lsh.add_song(row['name'], features)
            
            # Find similar songs for a sample
            print("\nSimilar songs analysis:")
            sample_songs = rows[:2]  # Analyze first 2 songs
            for row in sample_songs:
                features = self._extract_features(row)
                similar = self.lsh.find_similar_songs(features)
                print(f"\nSongs similar to '{row['name']}':")
                for s in similar[:3]:  # Show top 3 similar songs
                    print(f"- {s}")
                    
        except Exception as e:
            print(f"Error in similarity analysis: {e}")
    
    def show_basic_stats(self, df):
        """Show basic statistics about the batch"""
        try:
            print("\nArtist Statistics:")
            df.groupBy("artist") \
              .agg(
                  count("*").alias("track_count"),
                  round(avg("popularity"), 2).alias("avg_popularity"),
                  round(avg("danceability"), 2).alias("avg_danceability"),
                  round(avg("energy"), 2).alias("avg_energy")
              ) \
              .orderBy(desc("track_count")) \
              .show(5, truncate=False)
              
        except Exception as e:
            print(f"Error in basic stats: {e}")
    
    def _extract_features(self, row):
        """Extract and normalize features for similarity analysis"""
        return [
            float(row['danceability'] or 0),
            float(row['energy'] or 0),
            float(row['tempo'] or 0) / 200.0  # Normalize tempo
        ]

class BloomFilter:
    def __init__(self, size, num_hash_functions):
        self.size = size
        self.num_hash_functions = num_hash_functions
        self.bit_array = [0] * size
    
    def add(self, item):
        for seed in range(self.num_hash_functions):
            index = mmh3.hash(str(item), seed) % self.size
            self.bit_array[index] = 1
    
    def might_contain(self, item):
        return all(
            self.bit_array[mmh3.hash(str(item), seed) % self.size] == 1
            for seed in range(self.num_hash_functions)
        )

class ReservoirSampling:
    def __init__(self, k):
        self.k = k
        self.reservoir = []
        self.count = 0
    
    def add(self, item):
        self.count += 1
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
        else:
            j = random.randrange(self.count)
            if j < self.k:
                self.reservoir[j] = item
    
    def get_sample(self):
        return self.reservoir

class KAnonymity:
    def __init__(self, k):
        self.k = k
    
    def anonymize(self, data):
        if not data:
            return []
            
        anonymized = []
        # Sort data by popularity to group similar records
        sorted_data = sorted(data, key=lambda x: x['popularity'] if x['popularity'] is not None else -1)
        
        for i in range(0, len(sorted_data), self.k):
            group = sorted_data[i:i+self.k]
            if len(group) >= self.k:
                anonymized_record = {}
                for key in ['popularity', 'danceability', 'energy']:
                    try:
                        values = [float(r[key]) for r in group if r[key] is not None]
                        if values:
                            anonymized_record[key] = f"{min(values):.2f}-{max(values):.2f}"
                        else:
                            anonymized_record[key] = "N/A"
                    except (ValueError, TypeError):
                        anonymized_record[key] = "N/A"
                
                anonymized.extend([anonymized_record] * len(group))
        
        return anonymized

class DifferentialPrivacy:
    def __init__(self, epsilon):
        self.epsilon = epsilon
    
    def add_noise(self, value):
        """Add Laplace noise to ensure differential privacy"""
        if value is None:
            return 0.0
        scale = 1.0 / self.epsilon
        noise = np.random.laplace(0, scale)
        return float(value) + noise


def create_spark_session():
    return SparkSession \
        .builder \
        .appName("SpotifyAnalyticsAllComponents") \
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
    analytics = SpotifyAnalytics(spark)
    
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
