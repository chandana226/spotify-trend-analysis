Spotify Real-Time Analytics Platform
Project Overview
This project implements a real-time analytics platform for Spotify streaming data, featuring advanced streaming algorithms, privacy-preserving techniques, and comprehensive data analysis capabilities. The system processes real-time music streaming data through Apache Kafka and Apache Spark, providing insights while maintaining user privacy.

Technologies Used
Apache Kafka (3.8.0): Message streaming platform
Apache Spark (3.2.0): Distributed data processing
Python 3.12+: Primary programming language
Streamlit: Real-time dashboard visualization
Spotipy: Spotify API interaction

System Requirements

Java 8 or higher
Python 3.12 or higher
Apache Kafka 3.8.0
Apache Spark 3.2.0
Sufficient RAM (minimum 8GB recommended)

Installation and Setup
1. Install Dependencies
install -r requirements.txt
2. Configure Spotify API

Create an application at https://developer.spotify.com/dashboard
Update producer_real.py with your credentials:
client_id = 'your_client_id'
client_secret = 'your_client_secret'


3. Start Kafka Environment
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Server
bin/kafka-server-start.sh config/server.properties

Core Components
producer_real.py: Spotify data producer using Kafka
consumer_real.py: Main Spark streaming consumer
enhanced_consumer.py: Enhanced version with additional analytics
spotify_dash.py: Streamlit dashboard application

Analytics Modules

streaming_algorithms.py: Implementation of FM, Bloom Filter, and Reservoir Sampling
privacy_techniques.py: Differential Privacy and K-Anonymity implementations
lsh_implementation.py: Locality-Sensitive Hashing for song similarity
filters.py: Additional data filtering and processing

Analysis Components

F-M.py: Flajolet-Martin algorithm implementation
dgim_spotify.py: DGIM algorithm for streaming data
MUL.py: Machine Unlearning implementation

Features
Streaming Algorithms

Flajolet-Martin (FM) for cardinality estimation
Bloom Filter for membership testing
Reservoir Sampling for stream sampling
DGIM for sliding window queries

Privacy Preservation

Differential Privacy with configurable epsilon
K-Anonymity implementation
Feature masking capabilities
Secure data handling

Analytics Capabilities

Real-time track popularity analysis
Artist and genre distribution
Feature correlation analysis
Similarity detection using LSH

