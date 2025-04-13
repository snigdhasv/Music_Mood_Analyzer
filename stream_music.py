import requests
from kafka import KafkaProducer
import json
import librosa
import numpy as np
from yt_dlp import YoutubeDL
import os
from time import sleep
import subprocess
import traceback
import time
import hashlib
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Custom JSON encoder to handle NumPy types
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

# Configuration
LASTFM_API_KEY = "your_lastfm_api_key"
LASTFM_USER = "your_lastfm_username"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "song_stream"
AUDIO_CACHE_DIR = "audio_cache"
PROCESSED_CACHE = {}  # Track processed songs
LAST_SENT_FEATURES = {}  # Track last sent features
CHECK_INTERVAL = 5  # Check for updates every 5 seconds
DOWNLOAD_TIMEOUT = 20  # Maximum time to wait for download
MIN_UPDATE_INTERVAL = 60  # Minimum seconds between updates for same track
FEATURE_CHANGE_THRESHOLD = 0.05  # Minimum change in any feature to warrant update

# Check FFmpeg installation
def check_ffmpeg():
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, 
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except:
        logger.error("FFmpeg not found! Please install FFmpeg first.")
        logger.info("On Ubuntu/Debian: sudo apt install ffmpeg")
        logger.info("On macOS: brew install ffmpeg")
        logger.info("On Windows: Download from https://ffmpeg.org/")
        return False

if not check_ffmpeg():
    exit(1)

# Initialize Kafka Producer with custom JSON encoder
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, cls=NumpyEncoder).encode('utf-8')
)

def fetch_recent_tracks(limit=2):
    """Fetch last played tracks from Last.fM"""
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=user.getRecentTracks&user={LASTFM_USER}&api_key={LASTFM_API_KEY}&format=json&limit={limit}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'recenttracks' not in data or 'track' not in data['recenttracks']:
            logger.warning("Unexpected Last.fM API response format")
            return None
            
        return data['recenttracks']['track']
    except Exception as e:
        logger.error(f"Error fetching Last.fM data: {str(e)}")
        return None

def get_now_playing_track(tracks):
    """Identify the currently playing track"""
    for track in tracks:
        if '@attr' in track and track['@attr'].get('nowplaying') == 'true':
            return track, True  # Track is currently playing
    
    # If no track is currently playing, return the most recent completed track
    if tracks and len(tracks) > 0:
        return tracks[0], False  # Track is not currently playing
        
    return None, False

def create_track_id(track):
    """Create a unique ID for a track"""
    track_name = track.get('name', 'Unknown Track')
    artist = track['artist'].get('#text', 'Unknown Artist')
    album = track.get('album', {}).get('#text', '')
    
    # Create a hash for the track
    track_string = f"{track_name}_{artist}_{album}".lower()
    return hashlib.md5(track_string.encode()).hexdigest()

def download_audio(track_name, artist, force_download=False):
    """Download audio with improved search and caching"""
    os.makedirs(AUDIO_CACHE_DIR, exist_ok=True)
    
    # Create a safe filename
    safe_name = f"{artist}_{track_name}"[:100].replace("/", "_").replace("\\", "_")
    file_hash = hashlib.md5(f"{artist}_{track_name}".lower().encode()).hexdigest()
    output_path = os.path.join(AUDIO_CACHE_DIR, f"{file_hash}.mp3")
    
    # Check if already downloaded
    if os.path.exists(output_path) and not force_download:
        logger.info(f"Using cached audio for {artist} - {track_name}")
        return output_path
        
    # Simplify track name for better search results
    simple_track_name = track_name.split('(')[0].strip()
    
    # If we need to download
    logger.info(f"Downloading audio for {artist} - {simple_track_name}")
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': output_path.replace('.mp3', '.%(ext)s'),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
        }],
        'quiet': True,
        'noplaylist': True,
        'socket_timeout': DOWNLOAD_TIMEOUT,
        'default_search': 'ytsearch',
        'retries': 3,
    }
    
    try:
        with YoutubeDL(ydl_opts) as ydl:
            # Start a timer
            start_time = time.time()
            
            # Create a search query with simplified track name
            search_term = f"{artist} - {simple_track_name} audio"
            ydl.download([search_term])
            
            # Check download time
            elapsed = time.time() - start_time
            logger.info(f"Download completed in {elapsed:.1f} seconds")
            
        if os.path.exists(output_path):
            return output_path
        else:
            # Check if file exists with different extension
            possible_files = [os.path.join(AUDIO_CACHE_DIR, f) for f in os.listdir(AUDIO_CACHE_DIR) 
                              if f.startswith(file_hash)]
            if possible_files:
                logger.info(f"Found alternative file: {possible_files[0]}")
                return possible_files[0]
                
            logger.warning(f"Download succeeded but file not found at {output_path}")
            return None
            
    except Exception as e:
        logger.error(f"Download failed for {artist} - {simple_track_name}: {str(e)[:100]}...")
        return None

def extract_features(audio_path, duration=10):
    """Extract audio features with partial file analysis for speed"""
    if not audio_path or not os.path.exists(audio_path):
        logger.warning("Audio file does not exist for feature extraction")
        return None
        
    try:
        # Load just first few seconds for quick analysis
        y, sr = librosa.load(audio_path, duration=duration, sr=None)
        logger.info(f"Audio loaded - duration: {len(y)/sr:.2f}s, sample rate: {sr}Hz")
        
        # Extract key audio features
        features = {
            'spectral_centroid': float(np.mean(librosa.feature.spectral_centroid(y=y, sr=sr))),
            'spectral_rolloff': float(np.mean(librosa.feature.spectral_rolloff(y=y, sr=sr))),
            'zero_crossing_rate': float(np.mean(librosa.feature.zero_crossing_rate(y=y))),
            'tempo': 0.0,
            'energy': 0.0,
            'danceability': 0.0,
            'valence': 0.0,
            'acousticness': 0.0,
        }
        
        # Calculate derivative metrics (simplified versions)
        raw_energy = np.mean(librosa.feature.rms(y=y))
        normalized_energy = raw_energy / 0.1  # assuming 0.1 RMS as a "high" value
        features['energy'] = float(min(1.0, max(0.0, normalized_energy)))

        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
        if tempo > 180:
            logger.warning(f"Unusually high tempo detected: {tempo}. Halving to improve accuracy.")
            tempo = tempo / 2
        features['tempo'] = float(tempo)

        beat_strength = float(np.mean(librosa.beat.plp(y=y, sr=sr)))
        tempo_score = min(1.0, max(0.0, (features['tempo'] - 50) / 130))
        zcr_score = min(1.0, features['zero_crossing_rate'] * 1000)
        energy_score = min(1.0, features['energy'] * 100)

        features['danceability'] = float(min(1.0, max(0.0,
            0.4 * tempo_score +
            0.15 * zcr_score +
            0.15 * energy_score +
            0.3 * beat_strength
        )))

        valence = (
            0.3 * (features['spectral_rolloff'] / 8000) +
            0.2 * (features['spectral_centroid'] / 4000) + 
            0.2 * features['zero_crossing_rate'] * 10 -
            0.1 * features['danceability']
        )
        features['valence'] = float(min(1.0, max(0.0, valence)))

        acousticness = (
            0.5 * (1.0 - energy_score) +
            0.3 * (1.0 - features['spectral_rolloff'] / 6000) +
            0.2 * (1.0 - features['spectral_centroid'] / 3000)
        )
        features['acousticness'] = float(min(1.0, max(0.0, acousticness)))

        logger.info(f"🧪 Features for — V:{features['valence']:.2f}, E:{features['energy']:.2f}, T:{features['tempo']:.1f}, A:{features['acousticness']:.2f}, D:{features['danceability']:.2f}")
        logger.info("Extracted features successfully")
        return features
    except Exception as e:
        logger.error(f"Feature extraction failed: {str(e)}")
        traceback.print_exc()
        return None

def clean_cache(max_files=20):
    """Remove oldest files if cache gets too large"""
    try:
        files = os.listdir(AUDIO_CACHE_DIR)
        if len(files) <= max_files:
            return
            
        # Get files with creation time
        file_times = [(f, os.path.getctime(os.path.join(AUDIO_CACHE_DIR, f))) 
                      for f in files if f.endswith('.mp3')]
        
        # Sort by creation time (oldest first)
        file_times.sort(key=lambda x: x[1])
        
        # Remove oldest files
        for f, _ in file_times[:(len(file_times) - max_files)]:
            try:
                os.remove(os.path.join(AUDIO_CACHE_DIR, f))
                logger.info(f"Removed old cache file: {f}")
            except:
                pass
    except Exception as e:
        logger.error(f"Error cleaning cache: {str(e)}")

def clean_feature_cache(max_size=100):
    """Remove oldest entries if cache gets too large"""
    if len(LAST_SENT_FEATURES) > max_size:
        # Remove oldest entries based on PROCESSED_CACHE timestamps
        oldest_first = sorted(PROCESSED_CACHE.items(), key=lambda x: x[1])
        for track_id, _ in oldest_first[:len(LAST_SENT_FEATURES) - max_size]:
            LAST_SENT_FEATURES.pop(track_id, None)
            PROCESSED_CACHE.pop(track_id, None)

def process_and_stream():
    """Real-time processing loop that handles currently playing tracks"""
    last_track_id = None
    os.makedirs(AUDIO_CACHE_DIR, exist_ok=True)
    
    while True:
        try:
            logger.info(f"Checking for tracks at {time.strftime('%H:%M:%S')}...")
            
            # Fetch recent tracks
            tracks = fetch_recent_tracks(limit=2)
            if not tracks:
                logger.warning("No tracks received from Last.fm")
                sleep(CHECK_INTERVAL)
                continue
            
            # Get the currently playing or most recent track    
            track, is_now_playing = get_now_playing_track(tracks)
            if not track:
                logger.warning("No valid track found")
                sleep(CHECK_INTERVAL)
                continue
                
            # Extract track info
            track_name = track.get('name', 'Unknown Track')
            artist = track['artist'].get('#text', 'Unknown Artist')
            album = track.get('album', {}).get('#text', '')
            
            # Get track ID
            track_id = create_track_id(track)
            
            # Skip if we've processed this track recently and it's not currently playing
            if track_id == last_track_id and not is_now_playing:
                logger.info(f"No new tracks found (Last: {track_name} by {artist})")
                sleep(CHECK_INTERVAL)
                continue
                
            # For currently playing tracks, we want to update regularly
            # For completed tracks, we only process once
            should_process = True
            
            if track_id in PROCESSED_CACHE:
                last_processed_time = PROCESSED_CACHE[track_id]
                current_time = time.time()
                
                # Only process currently playing tracks every 30 seconds
                if is_now_playing and (current_time - last_processed_time) < 30:
                    should_process = False
                
                # Never reprocess completed tracks
                if not is_now_playing:
                    should_process = False
            
            if not should_process:
                logger.info(f"Skipping already processed track: {track_name} by {artist}")
                sleep(CHECK_INTERVAL)
                continue
                
            # Update tracking variables
            last_track_id = track_id
            PROCESSED_CACHE[track_id] = time.time()
            
            # Log what we're doing
            status = "currently playing" if is_now_playing else "recently completed"
            logger.info(f"\nProcessing {status} track: {track_name} by {artist}")
            
            # Clean caches occasionally
            clean_cache()
            clean_feature_cache()
            
            # Download and analyze
            audio_path = download_audio(track_name, artist)
            if not audio_path:
                logger.warning("Skipping due to download failure")
                sleep(CHECK_INTERVAL)
                continue
                
            # Extract features - use shorter duration for real-time
            analysis_duration = 10 if is_now_playing else 30
            features = extract_features(audio_path, duration=analysis_duration)
            
            # Only delete if not currently playing (keep cache for repeated analysis)
            if not is_now_playing:
                try:
                    os.remove(audio_path)
                except:
                    pass
            
            if features:
                # Check if features have changed significantly
                should_send = True
                if track_id in LAST_SENT_FEATURES:
                    last_features = LAST_SENT_FEATURES[track_id]
                    feature_changed = False
                    
                    # Compare each feature
                    for key in features:
                        if abs(features[key] - last_features.get(key, 0)) > FEATURE_CHANGE_THRESHOLD:
                            feature_changed = True
                            break
                            
                    # Also check time since last send
                    time_since_last = time.time() - PROCESSED_CACHE.get(track_id, 0)
                    should_send = feature_changed or (time_since_last >= MIN_UPDATE_INTERVAL)
                
                if should_send:
                    # Prepare the message
                    now = int(time.time())
                    message = {
                        'track': track_name,
                        'artist': artist,
                        'album': album,
                        'timestamp': now,
                        'status': 'playing' if is_now_playing else 'completed',
                        'features': features,
                        'image': next((img['#text'] for img in track['image'] if img['size'] == 'large'), None)
                    }
                    
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, message)
                    try:
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Sent to Kafka topic '{KAFKA_TOPIC}' (offset: {record_metadata.offset})")
                        LAST_SENT_FEATURES[track_id] = features.copy()  # Store sent features
                    except Exception as e:
                        logger.error(f"Kafka send failed: {str(e)}")
                else:
                    logger.info("Features unchanged - skipping Kafka send")
            
        except KeyboardInterrupt:
            logger.info("\nStopping pipeline...")
            break
        except Exception as e:
            logger.error(f"\nUnexpected error: {str(e)}")
            traceback.print_exc()
            
        # Check more frequently for real-time updates
        sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    logger.info("Starting Real-Time Music Analysis Pipeline (Ctrl+C to stop)...")
    try:
        process_and_stream()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    finally:
        producer.flush()
        producer.close()