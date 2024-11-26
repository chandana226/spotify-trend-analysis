import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
import json
import time

# Your updated credentials
client_id = '95a8aadaabde43e2977306a50410c381'
client_secret = 'dcb6c9927af244d2a293d52d6634c21d'

# Kafka settings
KAFKA_TOPIC = 'spotify_stream'
KAFKA_SERVER = 'localhost:9092'

def create_spotify_client():
    """Create Spotify client"""
    try:
        credentials = SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
        return spotipy.Spotify(client_credentials_manager=credentials)
    except Exception as e:
        print(f"Error creating Spotify client: {e}")
        raise

def create_kafka_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_current_tracks(spotify_client):
    """Get current top tracks"""
    playlist_id = '37i9dQZEVXbMDoHDwVN2tF'  # Global Top 50 playlist
    results = spotify_client.playlist_tracks(playlist_id)
    
    tracks = []
    for item in results['items']:
        track = item['track']
        
        # Get audio features for the track
        audio_features = spotify_client.audio_features(track['id'])[0]
        
        track_data = {
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'popularity': track['popularity'],
            'danceability': audio_features['danceability'],
            'energy': audio_features['energy'],
            'tempo': audio_features['tempo'],
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        tracks.append(track_data)
    
    return tracks

def main():
    print("Starting Spotify streaming producer...")
    
    try:
        # Test Spotify connection first
        spotify_client = create_spotify_client()
        test_playlist = spotify_client.playlist('37i9dQZEVXbMDoHDwVN2tF')
        print("✅ Spotify connection successful!")
        
        # Create Kafka producer
        kafka_producer = create_kafka_producer()
        print("✅ Kafka producer created!")
        
        while True:
            try:
                # Get current tracks
                tracks = get_current_tracks(spotify_client)
                
                # Send each track to Kafka
                for track in tracks:
                    kafka_producer.send(KAFKA_TOPIC, value=track)
                    print(f"Sent: {track['name']} by {track['artist']}")
                
                # Wait for 5 minutes before next update
                print("\nWaiting 5 minutes before next update...")
                time.sleep(300)
                
            except Exception as e:
                print(f"Error in streaming loop: {e}")
                time.sleep(5)
                continue
                
    except Exception as e:
        print(f"Startup error: {e}")
        
if __name__ == "__main__":
    main()
