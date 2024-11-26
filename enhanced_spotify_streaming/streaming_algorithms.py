import mmh3  # for Bloom Filter
import math
import random
from collections import defaultdict

class BloomFilter:
    def __init__(self, size, num_hash_functions):
        self.size = size
        self.num_hash_functions = num_hash_functions
        self.bit_array = [0] * size
    
    def add(self, item):
        for seed in range(self.num_hash_functions):
            index = mmh3.hash(str(item), seed) % self.size
            self.bit_array[index] = 1
    
    def check(self, item):
        for seed in range(self.num_hash_functions):
            index = mmh3.hash(str(item), seed) % self.size
            if self.bit_array[index] == 0:
                return False
        return True

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

def apply_streaming_algorithms(df):
    """Apply streaming algorithms to the data"""
    # Initialize Bloom Filter for popular songs (popularity > 90)
    bloom = BloomFilter(size=1000, num_hash_functions=3)
    
    # Initialize Reservoir Sampling for random song selection
    reservoir = ReservoirSampling(k=10)
    
    # Process each song
    for song in df.collect():
        # Add highly popular songs to Bloom Filter
        if song.popularity > 90:
            bloom.add(song.name)
        
        # Add all songs to reservoir sampling
        reservoir.add({
            'name': song.name,
            'artist': song.artist,
            'popularity': song.popularity
        })
    
    return {
        'bloom_filter': bloom,
        'reservoir_sample': reservoir.get_sample()
    }
