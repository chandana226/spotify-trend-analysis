# spotify_unlearning.py

from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
import numpy as np
import time
from datetime import datetime

class MachineUnlearning:
    def __init__(self, window_size=100):
        # Forgotten items tracking
        self.forgotten_tracks = set()
        self.forgotten_artists = set()
        self.forgotten_features = set()
        
        # Feature masks
        self.feature_masks = {
            'popularity': True,
            'danceability': True,
            'energy': True,
            'tempo': True
        }
        
        # Sliding window for active data
        self.window_size = window_size
        self.active_tracks = deque(maxlen=window_size)
        
        # Track history for verification
        self.track_history = defaultdict(list)
        self.unlearning_history = []
    
    def forget_track(self, track_name, reason=None):
        """Forget a specific track"""
        self.forgotten_tracks.add(track_name)
        self.unlearning_history.append({
            'type': 'track',
            'item': track_name,
            'timestamp': datetime.now(),
            'reason': reason
        })
        return f"Track '{track_name}' has been forgotten"
    
    def forget_artist(self, artist_name, reason=None):
        """Forget all tracks from an artist"""
        self.forgotten_artists.add(artist_name)
        self.unlearning_history.append({
            'type': 'artist',
            'item': artist_name,
            'timestamp': datetime.now(),
            'reason': reason
        })
        return f"Artist '{artist_name}' has been forgotten"
    
    def mask_feature(self, feature_name, reason=None):
        """Mask a specific feature"""
        if feature_name in self.feature_masks:
            self.feature_masks[feature_name] = False
            self.forgotten_features.add(feature_name)
            self.unlearning_history.append({
                'type': 'feature',
                'item': feature_name,
                'timestamp': datetime.now(),
                'reason': reason
            })
            return f"Feature '{feature_name}' has been masked"
        return f"Feature '{feature_name}' not found"

    def process_track(self, track_data):
        """Process a track with unlearning applied"""
        try:
            track_name = track_data.get('name', '')
            artist_name = track_data.get('artist', '')
            
            # Check if track or artist should be forgotten
            if track_name in self.forgotten_tracks or artist_name in self.forgotten_artists:
                return None
            
            # Apply feature masking
            processed_track = {}
            for key, value in track_data.items():
                if key in self.feature_masks:
                    if self.feature_masks[key]:
                        processed_track[key] = value
                    else:
                        processed_track[key] = None
                else:
                    processed_track[key] = value
            
            # Add to active tracks
            self.active_tracks.append(processed_track)
            
            # Update track history
            self.track_history[track_name].append({
                'timestamp': datetime.now(),
                'features': processed_track
            })
            
            return processed_track
            
        except Exception as e:
            print(f"Error processing track for unlearning: {e}")
            return None
    
    def get_unlearning_status(self):
        """Get current unlearning status"""
        return {
            'forgotten_tracks_count': len(self.forgotten_tracks),
            'forgotten_artists_count': len(self.forgotten_artists),
            'masked_features': [f for f, m in self.feature_masks.items() if not m],
            'active_tracks_count': len(self.active_tracks),
            'unlearning_history_count': len(self.unlearning_history)
        }
    
    def verify_unlearning(self, track_name=None):
        """Verify that unlearning has been applied correctly"""
        if track_name:
            # Check specific track
            is_forgotten = track_name in self.forgotten_tracks
            history = self.track_history.get(track_name, [])
            return {
                'track': track_name,
                'is_forgotten': is_forgotten,
                'history_entries': len(history),
                'last_seen': history[-1]['timestamp'] if history else None
            }
        else:
            # Overall verification
            return {
                'active_tracks': len(self.active_tracks),
                'forgotten_tracks': len(self.forgotten_tracks),
                'forgotten_artists': len(self.forgotten_artists),
                'masked_features': len(self.forgotten_features)
            }

class SpotifyUnlearningAnalyzer:
    def __init__(self, window_size=100):
        self.unlearning = MachineUnlearning(window_size)
        self.processed_count = 0
        self.start_time = datetime.now()
    
    def process_batch(self, tracks):
        """Process a batch of tracks"""
        results = []
        for track in tracks:
            processed = self.unlearning.process_track(track)
            if processed:
                results.append(processed)
                self.processed_count += 1
        
        self._print_status()
        return results
    
    def forget_item(self, item_type, item_name, reason=None):
        """Forget an item (track or artist)"""
        if item_type == 'track':
            return self.unlearning.forget_track(item_name, reason)
        elif item_type == 'artist':
            return self.unlearning.forget_artist(item_name, reason)
        elif item_type == 'feature':
            return self.unlearning.mask_feature(item_name, reason)
    
    def _print_status(self):
        """Print current status"""
        status = self.unlearning.get_unlearning_status()
        runtime = datetime.now() - self.start_time
        
        print("\n=== Unlearning Status ===")
        print(f"Runtime: {runtime}")
        print(f"Processed Tracks: {self.processed_count}")
        print(f"Forgotten Tracks: {status['forgotten_tracks_count']}")
        print(f"Forgotten Artists: {status['forgotten_artists_count']}")
        print("Masked Features:", status['masked_features'])
        print(f"Active Tracks: {status['active_tracks_count']}")

def main():
    print("Starting Spotify Unlearning Analysis...")
    
    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            'spotify_stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='unlearning_group'
        )
        
        # Initialize analyzer
        analyzer = SpotifyUnlearningAnalyzer(window_size=100)
        
        # Example: Pre-define some items to forget
        analyzer.forget_item('artist', 'Controversial Artist', 'User request')
        analyzer.forget_item('feature', 'popularity', 'Privacy concerns')
        
        print("Waiting for messages...")
        
        # Process messages
        batch = []
        for message in consumer:
            try:
                track_data = message.value
                batch.append(track_data)
                
                # Process in small batches
                if len(batch) >= 5:
                    analyzer.process_batch(batch)
                    batch = []
                    
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
