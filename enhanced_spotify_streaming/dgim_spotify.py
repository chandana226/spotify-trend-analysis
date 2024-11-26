# dgim_spotify.py

from kafka import KafkaConsumer
import json
from collections import deque
import time

class DGIMCounter:
    def __init__(self, window_size):
        self.window_size = window_size
        self.buckets = deque()  # [(timestamp, size)]
        self.timestamp = 0
        self.current_window = deque(maxlen=window_size)  # Fixed size window
    
    def add(self, bit, value_info=None):
        """Add a new bit to the window"""
        # Add to current window
        self.current_window.append((bit, value_info))
        
        # Add new bucket if bit is 1
        if bit == 1:
            self.buckets.append((self.timestamp, 1))
            self._merge_buckets()
        
        # Remove old buckets
        while self.buckets and (self.timestamp - self.buckets[0][0]) >= self.window_size:
            self.buckets.popleft()
            
        self.timestamp += 1
    
    def _merge_buckets(self):
        """Merge buckets of the same size"""
        i = len(self.buckets) - 1
        while i > 0:
            if len(self.buckets) < 2:
                break
                
            current = self.buckets[i]
            previous = self.buckets[i-1]
            
            if current[1] == previous[1]:  # If buckets have same size
                new_ts = min(current[0], previous[0])
                new_size = current[1] * 2
                self.buckets.pop()  # Remove current
                self.buckets.pop()  # Remove previous
                self.buckets.append((new_ts, new_size))
                i -= 2
            else:
                i -= 1
    
    def count_ones(self):
        """Count number of ones in the current window"""
        return sum(bucket[1] for bucket in self.buckets)
    
    def get_actual_count(self):
        """Get actual count for verification"""
        return sum(1 for bit, _ in self.current_window if bit == 1)
    
    def get_stats(self):
        """Get detailed statistics"""
        return {
            'estimated_count': self.count_ones(),
            'actual_count': self.get_actual_count(),
            'window_size': self.window_size,
            'current_timestamp': self.timestamp,
            'bucket_count': len(self.buckets)
        }

class SpotifyDGIMAnalyzer:
    def __init__(self, window_size=100):
        # Initialize counters for different metrics
        self.popularity_counter = DGIMCounter(window_size)
        self.energy_counter = DGIMCounter(window_size)
        self.dance_counter = DGIMCounter(window_size)
        
        # Set thresholds
        self.popularity_threshold = 80
        self.energy_threshold = 0.8
        self.dance_threshold = 0.8
        
        # Track counts
        self.total_tracks = 0
    
    def process_track(self, track_data):
        """Process a single track"""
        try:
            self.total_tracks += 1
            track_info = f"{track_data['name']} by {track_data['artist']}"
            
            # Convert metrics to binary signals
            popularity_bit = 1 if float(track_data.get('popularity', 0)) >= self.popularity_threshold else 0
            energy_bit = 1 if float(track_data.get('energy', 0)) >= self.energy_threshold else 0
            dance_bit = 1 if float(track_data.get('danceability', 0)) >= self.dance_threshold else 0
            
            # Update counters
            self.popularity_counter.add(popularity_bit, track_info)
            self.energy_counter.add(energy_bit, track_info)
            self.dance_counter.add(dance_bit, track_info)
            
            # Print analysis
            self._print_analysis(track_info)
            
        except Exception as e:
            print(f"Error processing track {track_data.get('name', 'Unknown')}: {str(e)}")
    
    def _print_analysis(self, current_track):
        """Print current analysis"""
        print(f"\nProcessing Track #{self.total_tracks}: {current_track}")
        print("-" * 60)
        
        # Popular tracks
        pop_stats = self.popularity_counter.get_stats()
        print("\nPopular Tracks (>= 80 popularity):")
        print(f"Estimated count: {pop_stats['estimated_count']}")
        print(f"Actual count: {pop_stats['actual_count']}")
        
        # High energy tracks
        energy_stats = self.energy_counter.get_stats()
        print("\nHigh Energy Tracks (>= 0.8 energy):")
        print(f"Estimated count: {energy_stats['estimated_count']}")
        print(f"Actual count: {energy_stats['actual_count']}")
        
        # Danceable tracks
        dance_stats = self.dance_counter.get_stats()
        print("\nDanceable Tracks (>= 0.8 danceability):")
        print(f"Estimated count: {dance_stats['estimated_count']}")
        print(f"Actual count: {dance_stats['actual_count']}")

def main():
    print("Starting Spotify DGIM Analysis...")
    
    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            'spotify_stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='dgim_group'
        )
        
        # Initialize analyzer
        analyzer = SpotifyDGIMAnalyzer(window_size=100)
        
        print("Waiting for messages...")
        
        # Process messages
        for message in consumer:
            try:
                track_data = message.value
                analyzer.process_track(track_data)
            except Exception as e:
                print(f"Error processing message: {e}")
            
    except KeyboardInterrupt:
        print("\nAnalysis stopped by user")
    except Exception as e:
        print(f"Error in main loop: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    main()
