from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import time
from collections import defaultdict, deque
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
MOOD_TOPIC = "mood_classified"
FEEDBACK_TOPIC = "user_feedback"
PLAYLIST_TOPIC = "playlist_recommendations"

# Window size for analysis (in seconds)
SLIDING_WINDOW_SIZE = 600  # 10 minutes

class PlaylistManager:
    def __init__(self):
        # Initialize Kafka consumers
        self.mood_consumer = KafkaConsumer(
            MOOD_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='playlist_manager'
        )
        
        self.feedback_consumer = KafkaConsumer(
            FEEDBACK_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='playlist_manager'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Data structures to store recent songs and feedback
        self.recent_songs = deque(maxlen=100)  # Store recent songs
        self.mood_history = defaultdict(int)   # Count of songs by mood
        self.mood_feedback = defaultdict(list) # Feedback for each mood
        
        # For sliding window analysis
        self.window_start_time = time.time()
        
        # Thread control
        self.running = False
        self.threads = []
    
    def start(self):
        """Start the playlist manager threads"""
        if self.running:
            logger.warning("Playlist manager is already running")
            return
            
        self.running = True
        
        # Start mood consumer thread
        mood_thread = threading.Thread(target=self._process_moods)
        mood_thread.daemon = True
        mood_thread.start()
        self.threads.append(mood_thread)
        
        # Start feedback consumer thread
        feedback_thread = threading.Thread(target=self._process_feedback)
        feedback_thread.daemon = True
        feedback_thread.start()
        self.threads.append(feedback_thread)
        
        # Start analysis thread
        analysis_thread = threading.Thread(target=self._analyze_and_recommend)
        analysis_thread.daemon = True
        analysis_thread.start()
        self.threads.append(analysis_thread)
        
        logger.info("🎯 Started playlist manager")
    
    def stop(self):
        """Stop the playlist manager"""
        self.running = False
        for thread in self.threads:
            thread.join(timeout=5.0)
        self.mood_consumer.close()
        self.feedback_consumer.close()
        self.producer.flush()
        self.producer.close()
        logger.info("Stopped playlist manager")
    
    def _process_moods(self):
        """Process incoming mood classifications"""
        try:
            for message in self.mood_consumer:
                if not self.running:
                    break
                    
                song = message.value
                mood = song.get('mood', 'unknown')
                
                # Add to recent songs
                self.recent_songs.append({
                    'track': song.get('track', 'Unknown'),
                    'artist': song.get('artist', 'Unknown'),
                    'mood': mood,
                    'timestamp': time.time()
                })
                
                # Update mood history
                self.mood_history[mood] += 1
                
                logger.debug(f"Added song to playlist manager: {song.get('track')} ({mood})")
        except Exception as e:
            logger.error(f"Error processing moods: {str(e)}")
            self.running = False
    
    def _process_feedback(self):
        """Process incoming user feedback"""
        try:
            for message in self.feedback_consumer:
                if not self.running:
                    break
                    
                feedback = message.value
                mood = feedback.get('mood', 'unknown')
                
                # Store feedback by mood
                self.mood_feedback[mood].append({
                    'value': feedback.get('feedback_value', 0.0),
                    'action': feedback.get('action', 'unknown'),
                    'timestamp': time.time()
                })
                
                logger.debug(f"Added feedback for mood: {mood} ({feedback.get('action')})")
        except Exception as e:
            logger.error(f"Error processing feedback: {str(e)}")
            self.running = False
    
    def _analyze_and_recommend(self):
        """Analyze recent data and make playlist recommendations"""
        while self.running:
            try:
                current_time = time.time()
                
                # Check if we've reached the end of our sliding window
                if current_time - self.window_start_time >= SLIDING_WINDOW_SIZE:
                    # Generate recommendations based on recent data
                    recommendations = self._generate_recommendations()
                    
                    if recommendations:
                        # Send recommendations to Kafka
                        self.producer.send(PLAYLIST_TOPIC, {
                            'timestamp': current_time,
                            'window_size': SLIDING_WINDOW_SIZE,
                            'recommendations': recommendations
                        })
                        
                        logger.info(f"📊 Generated playlist recommendations: {recommendations}")
                    
                    # Reset the window start time
                    self.window_start_time = current_time
                    
                    # Clean up old feedback data
                    self._clean_old_data(current_time)
                
                # Sleep to avoid excessive CPU usage
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in analysis loop: {str(e)}")
                time.sleep(30)  # Longer sleep on error
    
    def _generate_recommendations(self):
        """Generate mood-based playlist recommendations"""
        # Calculate mood preferences based on feedback
        mood_preferences = {}
        
        # Process feedback for each mood
        for mood, feedback_list in self.mood_feedback.items():
            # Only consider recent feedback
            recent_feedback = [f for f in feedback_list 
                               if time.time() - f.get('timestamp', 0) < SLIDING_WINDOW_SIZE]
            
            if not recent_feedback:
                # No recent feedback for this mood
                mood_preferences[mood] = 0.0
                continue
                
            # Calculate average feedback value
            avg_feedback = sum(f.get('value', 0.0) for f in recent_feedback) / len(recent_feedback)
            mood_preferences[mood] = avg_feedback
        
        # Calculate overall mood distribution in the current playlist
        total_songs = sum(self.mood_history.values())
        current_distribution = {mood: count / total_songs if total_songs > 0 else 0 
                               for mood, count in self.mood_history.items()}
        
        # Generate recommendations (adjust distribution based on preferences)
        target_distribution = {}
        
        for mood in set(list(current_distribution.keys()) + list(mood_preferences.keys())):
            current = current_distribution.get(mood, 0.0)
            preference = mood_preferences.get(mood, 0.0)
            
            # Adjust distribution based on preference
            # If preference is positive, increase the mood's presence
            # If preference is negative, decrease the mood's presence
            adjustment = preference * 0.3  # Scale factor to avoid extreme changes
            target = max(0.0, min(1.0, current + adjustment))
            
            target_distribution[mood] = target
        
        # Normalize distribution to sum to 1.0
        total = sum(target_distribution.values())
        if total > 0:
            target_distribution = {mood: value / total for mood, value in target_distribution.items()}
        
        return target_distribution
    
    def _clean_old_data(self, current_time):
        """Clean up data older than the sliding window"""
        cutoff_time = current_time - SLIDING_WINDOW_SIZE
        
        # Clean old feedback
        for mood in list(self.mood_feedback.keys()):
            self.mood_feedback[mood] = [f for f in self.mood_feedback[mood] 
                                        if f.get('timestamp', 0) >= cutoff_time]
        
        # Reset mood history if the window has passed
        self.mood_history.clear()
        
        # Update mood history based on songs in the current window
        for song in self.recent_songs:
            if song.get('timestamp', 0) >= cutoff_time:
                self.mood_history[song.get('mood', 'unknown')] += 1

def main():
    """Run the playlist manager"""
    manager = PlaylistManager()
    
    try:
        manager.start()
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Playlist manager stopped by user")
    finally:
        manager.stop()

if __name__ == "__main__":
    main()