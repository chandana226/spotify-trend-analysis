# spotify_dash.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from kafka import KafkaConsumer
import json
from collections import deque
import threading
import time

# Initialize session state
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    st.session_state.messages = deque(maxlen=100)
    st.session_state.artist_counts = {}
    st.session_state.track_history = deque(maxlen=100)
    st.session_state.kafka_running = False
    st.session_state.debug_messages = []

class SpotifyDashboard:
    def __init__(self):
        st.set_page_config(
            page_title="Spotify Analytics",
            page_icon="🎵",
            layout="wide"
        )
        st.title("🎵 Spotify Real-Time Analytics")
        
        # Debug section
        st.sidebar.title("Debug Info")
        if st.sidebar.button("Clear Debug Messages"):
            st.session_state.debug_messages = []
        
        # Start Kafka consumer
        if not st.session_state.kafka_running:
            self.start_kafka_consumer()
            self.add_debug_message("Started Kafka consumer")
    
    def add_debug_message(self, message):
        """Add debug message with timestamp"""
        timestamp = time.strftime("%H:%M:%S")
        st.session_state.debug_messages.append(f"{timestamp}: {message}")
    
    def start_kafka_consumer(self):
        def kafka_consumer():
            try:
                self.add_debug_message("Connecting to Kafka...")
                consumer = KafkaConsumer(
                    'spotify_stream',
                    bootstrap_servers=['localhost:9092'],
                    auto_offset_reset='latest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    group_id='dashboard_group',
                    consumer_timeout_ms=1000  # 1 second timeout
                )
                self.add_debug_message("Successfully connected to Kafka")
                
                while st.session_state.kafka_running:
                    try:
                        message = next(consumer)
                        track_data = message.value
                        st.session_state.messages.append(track_data)
                        
                        # Update artist counts
                        artist = track_data['artist']
                        if artist in st.session_state.artist_counts:
                            st.session_state.artist_counts[artist] += 1
                        else:
                            st.session_state.artist_counts[artist] = 1
                        
                        self.add_debug_message(f"Received track: {track_data['name']}")
                    except StopIteration:
                        time.sleep(1)  # Wait before next attempt
                    except Exception as e:
                        self.add_debug_message(f"Error processing message: {str(e)}")
                
            except Exception as e:
                self.add_debug_message(f"Kafka Error: {str(e)}")
        
        st.session_state.kafka_running = True
        threading.Thread(target=kafka_consumer, daemon=True).start()
    
    def show_debug_info(self):
        """Show debug information"""
        st.sidebar.write("Latest Debug Messages:")
        for msg in list(st.session_state.debug_messages)[-10:]:  # Show last 10 messages
            st.sidebar.text(msg)
        
        # Show current state
        st.sidebar.write("\nCurrent State:")
        st.sidebar.write(f"Messages in queue: {len(st.session_state.messages)}")
        st.sidebar.write(f"Unique artists: {len(st.session_state.artist_counts)}")
        st.sidebar.write(f"Kafka running: {st.session_state.kafka_running}")
    
    def show_metrics(self):
        col1, col2, col3 = st.columns(3)
        
        # Current Track
        with col1:
            if len(st.session_state.messages) > 0:
                current_track = list(st.session_state.messages)[-1]
                st.metric(
                    "Now Playing",
                    current_track['name'],
                    current_track['artist']
                )
                self.add_debug_message(f"Showing current track: {current_track['name']}")
            else:
                st.metric("Now Playing", "Waiting for data...", "")
                self.add_debug_message("No tracks to display")
        
        # Artist Count
        with col2:
            st.metric(
                "Unique Artists",
                len(st.session_state.artist_counts)
            )
        
        # Average Popularity
        with col3:
            if len(st.session_state.messages) > 0:
                avg_pop = np.mean([
                    track['popularity'] 
                    for track in st.session_state.messages
                ])
                st.metric("Average Popularity", f"{avg_pop:.1f}")
    
    def show_charts(self):
        if len(st.session_state.messages) > 0:
            # Convert to DataFrame
            df = pd.DataFrame(list(st.session_state.messages))
            
            # Show raw data for debugging
            if st.checkbox("Show raw data"):
                st.write(df)
            
            # Popularity Trend
            fig_pop = px.line(
                df, y='popularity',
                title="Popularity Trend"
            )
            st.plotly_chart(fig_pop)
            
            # Artist Distribution
            artist_df = pd.DataFrame(
                list(st.session_state.artist_counts.items()),
                columns=['Artist', 'Count']
            )
            fig_artists = px.bar(
                artist_df,
                x='Artist',
                y='Count',
                title="Artist Distribution"
            )
            st.plotly_chart(fig_artists)
            
            # Feature Correlation
            if len(df) > 1:
                corr = df[['popularity', 'danceability', 'energy']].corr()
                fig_corr = px.imshow(
                    corr,
                    title="Feature Correlation"
                )
                st.plotly_chart(fig_corr)
        else:
            st.write("Waiting for data...")
            self.add_debug_message("No data available for charts")
    
    def run(self):
        # Show debug info
        self.show_debug_info()
        
        # Show metrics
        self.show_metrics()
        
        # Show charts
        self.show_charts()
        
        # Add status indicators
        st.sidebar.write("\nStatus:")
        st.sidebar.write("✅ Dashboard Running")
        st.sidebar.write("✅ Kafka Connected" if st.session_state.kafka_running else "❌ Kafka Not Connected")
        st.sidebar.write(f"✅ Data Receiving ({len(st.session_state.messages)} tracks)" if len(st.session_state.messages) > 0 else "❌ No Data Received")
        
        # Auto-refresh
        time.sleep(1)
        st.experimental_rerun()

def main():
    dashboard = SpotifyDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()
