import numpy as np
from datasketch import MinHash, MinHashLSH

class SpotifyLSH:
    def __init__(self, threshold=0.5):
        self.lsh = MinHashLSH(threshold=threshold)
        self.minhashes = {}
        
    def _create_feature_vector(self, song):
        """Create feature vector from song attributes"""
        return [
            song.danceability,
            song.energy,
            song.tempo / 200.0  # Normalize tempo
        ]
    
    def _create_minhash(self, feature_vector, num_perm=128):
        """Create MinHash from feature vector"""
        m = MinHash(num_perm=num_perm)
        for i, value in enumerate(feature_vector):
            # Convert continuous values to discrete features
            discretized = str(round(value * 100))
            m.update(f"{i}:{discretized}".encode('utf-8'))
        return m
    
    def add_song(self, song):
        """Add a song to the LSH index"""
        features = self._create_feature_vector(song)
        minhash = self._create_minhash(features)
        
        # Store minhash
        song_id = f"{song.name}-{song.artist}"
        self.minhashes[song_id] = minhash
        
        # Add to LSH
        self.lsh.insert(song_id, minhash)
    
    def find_similar_songs(self, query_song):
        """Find similar songs using LSH"""
        features = self._create_feature_vector(query_song)
        query_minhash = self._create_minhash(features)
        
        # Query LSH
        return self.lsh.query(query_minhash)

def apply_lsh(df):
    """Apply LSH to find similar songs"""
    lsh = SpotifyLSH(threshold=0.7)
    
    # Add all songs to LSH
    for song in df.collect():
        lsh.add_song(song)
    
    # Find similar songs for each song
    similar_songs = {}
    for song in df.collect():
        similar = lsh.find_similar_songs(song)
        if similar:
            similar_songs[f"{song.name}-{song.artist}"] = similar
    
    return similar_songs
