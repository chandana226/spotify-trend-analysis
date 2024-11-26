import numpy as np
from collections import defaultdict

class DifferentialPrivacy:
    def __init__(self, epsilon=1.0):
        self.epsilon = epsilon
    
    def add_laplace_noise(self, value):
        """Add Laplace noise to ensure differential privacy"""
        scale = 1.0 / self.epsilon
        noise = np.random.laplace(0, scale)
        return value + noise

class KAnonymity:
    def __init__(self, k=3):
        self.k = k
    
    def anonymize(self, data, quasi_identifiers):
        """
        Implement k-anonymity by generalizing quasi-identifiers
        until each combination appears at least k times
        """
        groups = defaultdict(list)
        
        # Group records by quasi-identifier values
        for record in data:
            key = tuple(record[qi] for qi in quasi_identifiers)
            groups[key].append(record)
        
        # Generalize groups that don't meet k-anonymity
        anonymized_data = []
        for group in groups.values():
            if len(group) >= self.k:
                anonymized_data.extend(group)
            else:
                # Generalize the quasi-identifiers
                generalized_record = self._generalize_group(group)
                anonymized_data.extend([generalized_record] * len(group))
        
        return anonymized_data
    
    def _generalize_group(self, group):
        """Generalize a group of records that don't meet k-anonymity"""
        generalized = group[0].copy()
        
        # Generalize numerical values to ranges
        for key in generalized:
            if isinstance(generalized[key], (int, float)):
                values = [r[key] for r in group]
                generalized[key] = f"{min(values)}-{max(values)}"
        
        return generalized

def apply_privacy_techniques(df):
    """Apply privacy techniques to the streaming data"""
    # Initialize privacy tools
    dp = DifferentialPrivacy(epsilon=0.5)
    k_anon = KAnonymity(k=3)
    
    # Convert DataFrame to list of dictionaries for processing
    records = df.collect()
    data = [
        {
            'artist': r.artist,
            'popularity': r.popularity,
            'danceability': r.danceability,
            'energy': r.energy
        }
        for r in records
    ]
    
    # Apply k-anonymity
    quasi_identifiers = ['popularity', 'danceability', 'energy']
    anonymized_data = k_anon.anonymize(data, quasi_identifiers)
    
    # Apply differential privacy to aggregated statistics
    private_stats = {
        'avg_popularity': dp.add_laplace_noise(df.agg({'popularity': 'avg'}).collect()[0][0]),
        'avg_danceability': dp.add_laplace_noise(df.agg({'danceability': 'avg'}).collect()[0][0]),
        'avg_energy': dp.add_laplace_noise(df.agg({'energy': 'avg'}).collect()[0][0])
    }
    
    return {
        'anonymized_data': anonymized_data,
        'private_stats': private_stats
    }
