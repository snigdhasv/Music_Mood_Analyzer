import json
import time
import requests
from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth

import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API configuration
LASTFM_API_KEY = "c297e8c817537bb040134da565336e3c"
LASTFM_USER = "snigdhasv"  # Replace with your Last.fm username
SPOTIFY_CLIENT_ID = "ae1aa8a3d64e40e09598ea17a1537f2f"
SPOTIFY_CLIENT_SECRET = "f500311fc12b4d33b53272427b6b00de"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Update with your Kafka broker address
KAFKA_TOPIC = 'song_stream'

class MusicPipeline:
    def __init__(self):
        # Initialize Spotify client
        logger.info("Initializing Spotify client...")
        try:
            client_credentials_manager = SpotifyClientCredentials(
                client_id=SPOTIFY_CLIENT_ID,
                client_secret=SPOTIFY_CLIENT_SECRET
            )
            
            self.spotify = spotipy.Spotify(auth_manager=client_credentials_manager)
            # Test the connection
            # Test authentication with a public endpoint
            self.spotify.search(q="test", limit=1)  # This will fail if auth is incorrect
            logger.info("Spotify client initialized with client credentials flow")
        except Exception as e:
            logger.error(f"Failed to initialize Spotify client: {str(e)}")
            # Continue anyway, we'll handle errors per-request
            self.spotify = None
        
        # Initialize Kafka producer
        try:
            logger.info("Initializing Kafka producer...")
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            self.producer = None
        
        # Track the most recent timestamp to avoid duplicates
        self.last_timestamp = 0
        self.processed_tracks = set()  # Track recently processed tracks to avoid duplicates
        
    def get_lastfm_recent_tracks(self):
        """Fetch recent tracks from Last.fm API"""
        url = "http://ws.audioscrobbler.com/2.0/"
        params = {
            'method': 'user.getrecenttracks',
            'user': LASTFM_USER,
            'api_key': LASTFM_API_KEY,
            'format': 'json',
            'limit': 5,
            'from': self.last_timestamp  # Only get tracks since last check
        }
        
        try:
            logger.info(f"Fetching recent tracks for Last.fm user: {LASTFM_USER}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            tracks_data = response.json()
            tracks = tracks_data['recenttracks']['track']
            logger.info(f"Received {len(tracks)} tracks from Last.fm")
            
            # Update the timestamp if we have tracks
            if tracks and '@attr' in tracks[0] and 'nowplaying' in tracks[0]['@attr']:
                # Currently playing track
                new_tracks = [tracks[0]]
                logger.info(f"Currently playing: {tracks[0]['artist']['#text']} - {tracks[0]['name']}")
            elif tracks:
                # Filter out tracks that we've already processed
                new_tracks = []
                for track in tracks:
                    # Create a unique identifier for the track
                    track_id = f"{track['artist']['#text']}_{track['name']}"
                    
                    # If track has a date, use it to update our timestamp
                    if 'date' in track and 'uts' in track['date']:
                        timestamp = int(track['date']['uts'])
                        if timestamp > self.last_timestamp:
                            self.last_timestamp = timestamp
                    
                    # Only include tracks we haven't processed yet
                    if track_id not in self.processed_tracks:
                        new_tracks.append(track)
                        self.processed_tracks.add(track_id)
                        
                        # Keep the processed tracks set from growing indefinitely
                        if len(self.processed_tracks) > 100:
                            self.processed_tracks.pop()
            else:
                new_tracks = []
                
            return new_tracks
        
        except Exception as e:
            logger.error(f"Error fetching Last.fm data: {str(e)}")
            return []
    
    def get_spotify_audio_features(self, artist, track_name):
        """Get Spotify audio features for a track"""
        if not self.spotify:
            logger.warning("Spotify client not initialized - skipping audio features")
            return self.create_basic_track_data(artist, track_name)
            
        try:
            # Search for the track on Spotify
            logger.info(f"Searching Spotify for: {artist} - {track_name}")
            query = f"artist:{artist} track:{track_name}"
            results = self.spotify.search(q=query, type='track', limit=1)
            
            if not results['tracks']['items']:
                logger.warning(f"No Spotify data found for: {artist} - {track_name}")
                return self.create_basic_track_data(artist, track_name)
            
            # Get the track object
            track = results['tracks']['items'][0]
            
            # Create a basic enriched data object without audio features
            # in case the audio features request fails
            enriched_data = {
                'title': track['name'],
                'artist': track['artists'][0]['name'],
                'album': track['album']['name'],
                'release_date': track['album'].get('release_date', 'Unknown'),
                'popularity': track['popularity'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit'],
                'spotify_id': track['id'],
                'spotify_url': track['external_urls']['spotify'],
                
                # Track source and timestamp
                'source': 'lastfm',
                'timestamp': int(time.time())
            }
            
            # Try to get audio features - if it fails, we'll still have basic track info
            try:
                logger.info(f"Fetching audio features for track ID: {track['id']}")
                audio_features = self.spotify.audio_features(track['id'])
                
                if audio_features and audio_features[0]:
                    features = audio_features[0]
                    # Add audio features to our enriched data
                    enriched_data.update({
                        'danceability': features['danceability'],
                        'energy': features['energy'],
                        'key': features['key'],
                        'loudness': features['loudness'],
                        'mode': features['mode'],
                        'speechiness': features['speechiness'],
                        'acousticness': features['acousticness'],
                        'instrumentalness': features['instrumentalness'],
                        'liveness': features['liveness'],
                        'valence': features['valence'],  # Happiness measure
                        'tempo': features['tempo']
                    })
                    logger.info("Successfully retrieved audio features")
                else:
                    logger.warning(f"No audio features returned for track ID: {track['id']}")
            except Exception as e:
                logger.error(f"Error fetching audio features: {str(e)}")
                # Continue with basic track info
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Error in Spotify processing for {artist} - {track_name}: {str(e)}")
            return self.create_basic_track_data(artist, track_name)
    
    def create_basic_track_data(self, artist, track_name):
        """Create a basic track data object when Spotify data is unavailable"""
        return {
            'title': track_name,
            'artist': artist,
            'source': 'lastfm',
            'timestamp': int(time.time()),
            'spotify_data_available': False
        }
    
    def process_and_stream(self):
        """Main pipeline function to process tracks and stream to Kafka"""
        tracks = self.get_lastfm_recent_tracks()
        
        if not tracks:
            logger.info("No new tracks found")
            return 0
        
        processed_count = 0
        for track in tracks:
            artist = track['artist']['#text']
            track_name = track['name']
            
            logger.info(f"Processing: {artist} - {track_name}")
            
            # Get enriched data from Spotify
            enriched_data = self.get_spotify_audio_features(artist, track_name)
            
            if enriched_data:
                # Add Last.fm specific data
                if 'image' in track and track['image']:
                    enriched_data['lastfm_image'] = next((img['#text'] for img in track['image'] if img['size'] == 'large'), None)
                
                if 'date' in track:
                    enriched_data['lastfm_timestamp'] = track['date']['uts'] if 'uts' in track['date'] else None
                
                # Try to send to Kafka if producer is available
                if self.producer:
                    try:
                        self.producer.send(KAFKA_TOPIC, value=enriched_data)
                        processed_count += 1
                        logger.info(f"Sent to Kafka: {artist} - {track_name}")
                        
                        # Optional: Print some audio features for debugging
                        if 'energy' in enriched_data:
                            logger.info(f"  Energy: {enriched_data.get('energy', 'N/A')}, Valence: {enriched_data.get('valence', 'N/A')}")
                    except Exception as e:
                        logger.error(f"Failed to send to Kafka: {str(e)}")
                else:
                    # Just log the data if Kafka is not available
                    logger.info(f"Would send to Kafka (if available): {artist} - {track_name}")
                    processed_count += 1
        
        # Make sure all messages are sent if producer exists
        if self.producer:
            try:
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error flushing Kafka messages: {str(e)}")
                
        return processed_count

    def run_continuous_pipeline(self, interval=30):
        """Run the pipeline continuously with the specified interval (in seconds)"""
        logger.info(f"Starting continuous pipeline, checking every {interval} seconds...")
        try:
            while True:
                count = self.process_and_stream()
                logger.info(f"Processed {count} new tracks. Waiting {interval} seconds...")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        finally:
            # Close the Kafka producer if it exists
            if self.producer:
                self.producer.close()
                logger.info("Kafka producer closed")


if __name__ == "__main__":
    pipeline = MusicPipeline()
    
    # For testing, you can run just once
    # pipeline.process_and_stream()
    
    # For continuous operation
    pipeline.run_continuous_pipeline(interval=30)  # Check every 30 seconds