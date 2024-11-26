# spotify_fm_analysis.py

import mmh3
import numpy as np
from tabulate import tabulate
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time

class SpotifyFM:
    def __init__(self, client_id, client_secret):
        """Initialize with Spotify credentials"""
        self.spotify = spotipy.Spotify(
            client_credentials_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
        )
        self.fm_estimator = FlajoletMartin(num_estimators=32)
        
    def get_playlist_tracks(self, playlist_id):
        """Get tracks from a Spotify playlist"""
        results = self.spotify.playlist_tracks(playlist_id)
        tracks = []
        
        print("\nFetching playlist data...")
        for item in results['items']:
            if item['track']:
                track_info = {
                    'name': item['track']['name'],
                    'artist': item['track']['artists'][0]['name'],
                    'popularity': item['track']['popularity']
                }
                tracks.append(track_info)
                print(f"Found track: {track_info['name']} by {track_info['artist']}")
        
        return tracks
    
    def analyze_unique_tracks(self, playlist_id):
        """Analyze unique tracks in a playlist using FM algorithm"""
        tracks = self.get_playlist_tracks(playlist_id)
        
        print("\n=== Playlist Analysis ===")
        print(f"Total tracks fetched: {len(tracks)}")
        
        # Get actual unique count
        unique_tracks = set(track['name'] for track in tracks)
        print(f"Actual unique tracks: {len(unique_tracks)}")
        
        # Use FM algorithm
        for track in tracks:
            self.fm_estimator.add(track['name'])
        
        estimate = self.fm_estimator.estimate()
        error_percentage = abs(estimate - len(unique_tracks)) / len(unique_tracks) * 100
        
        print("\nFlajolet-Martin Analysis Results:")
        print(f"Estimated unique tracks: {estimate:.2f}")
        print(f"Error percentage: {error_percentage:.2f}%")
        
        # Show top artists
        artist_counts = {}
        for track in tracks:
            artist = track['artist']
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        
        print("\nTop Artists in Playlist:")
        sorted_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for artist, count in sorted_artists:
            print(f"- {artist}: {count} tracks")

class FlajoletMartin:
    """Flajolet-Martin Algorithm Implementation"""
    def __init__(self, num_estimators=32):
        self.num_estimators = num_estimators
        self.max_zeros = [0] * num_estimators
        self.item_history = {}
    
    def add(self, item):
        if item not in self.item_history:
            self.item_history[item] = []
        
        for i in range(self.num_estimators):
            hash_val = mmh3.hash(str(item), seed=i)
            binary = bin(hash_val)[2:] if hash_val >= 0 else bin(hash_val)[3:]
            trailing_zeros = len(binary) - len(binary.rstrip('0'))
            
            self.item_history[item].append({
                'estimator': i,
                'hash_value': hash_val,
                'trailing_zeros': trailing_zeros
            })
            
            self.max_zeros[i] = max(self.max_zeros[i], trailing_zeros)
    
    def estimate(self):
        avg_zeros = sum(self.max_zeros) / self.num_estimators
        return 2 ** avg_zeros
    
    def explain_estimate(self):
        """Explain the estimation process"""
        print("\n=== Estimation Process Details ===")
        print(f"Number of estimators used: {self.num_estimators}")
        print(f"Average trailing zeros: {sum(self.max_zeros) / self.num_estimators:.2f}")
        print("\nSample of processed items:")
        
        # Show details for first 3 items
        for i, (item, hashes) in enumerate(list(self.item_history.items())[:3]):
            print(f"\nItem {i+1}: {item}")
            data = []
            for h in hashes[:3]:  # Show first 3 hash functions
                data.append([
                    h['estimator'],
                    h['hash_value'],
                    h['trailing_zeros']
                ])
            print(tabulate(data, 
                         headers=['Hash Function', 'Hash Value', 'Trailing Zeros'],
                         tablefmt='grid'))

def main():
    # Your Spotify API credentials
    CLIENT_ID = '95a8aadaabde43e2977306a50410c381'
    CLIENT_SECRET = 'dcb6c9927af244d2a293d52d6634c21d'
    
    # Initialize SpotifyFM analyzer
    spotify_fm = SpotifyFM(CLIENT_ID, CLIENT_SECRET)
    
    # Analyze different playlists
    playlists = [
        ('37i9dQZEVXbMDoHDwVN2tF', 'Global Top 50'),
        ('37i9dQZEVXbLRQDuF5jeBp', 'US Top 50'),
    ]
    
    for playlist_id, name in playlists:
        print(f"\n=== Analyzing Playlist: {name} ===")
        spotify_fm.analyze_unique_tracks(playlist_id)
        spotify_fm.fm_estimator.explain_estimate()
        time.sleep(1)  # Respect API rate limits

if __name__ == "__main__":
    main()
