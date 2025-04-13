from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import random
import time
from datetime import datetime
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
MOOD_TOPIC = "mood_classified"
FEEDBACK_TOPIC = "user_feedback"

# Feedback preferences (can be adjusted to simulate different user preferences)
MOOD_PREFERENCES = {
    "happy": 0.8,     # 80% chance of positive feedback for happy songs
    "energetic": 0.7, # 70% chance of positive feedback for energetic songs
    "calm": 0.6,      # 60% chance of positive feedback for calm songs
    "sad": 0.3,       # 30% chance of positive feedback for sad songs
    "dark": 0.2,      # 20% chance of positive feedback for dark songs
    "neutral": 0.5    # 50% chance of positive feedback for neutral songs
}

class FeedbackSimulator:
    def __init__(self):
        self.consumer = KafkaConsumer(
            MOOD_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='feedback_simulator'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.running = False
        self.thread = None
        
    def generate_feedback(self, song):
        """Generate simulated user feedback based on song mood"""
        mood = song.get('mood', 'neutral')
        like_probability = MOOD_PREFERENCES.get(mood, 0.5)
        
        # Determine feedback action
        random_value = random.random()
        
        if random_value < like_probability:
            action = random.choice(["like", "listen_full", "add_to_playlist"])
            feedback_value = 1.0
        else:
            action = random.choice(["skip", "dislike", "reduce_volume"])
            feedback_value = -1.0
            
        # Add some random noise to make it more realistic
        feedback_strength = min(1.0, max(-1.0, feedback_value + random.uniform(-0.2, 0.2)))
        
        return {
            "track_id": f"{song.get('artist', 'Unknown')} - {song.get('track', 'Unknown')}",
            "timestamp": datetime.now().isoformat(),
            "mood": mood,
            "action": action,
            "feedback_value": feedback_strength,
            "user_id": "simulated_user_1"  # In a real system, this would be a real user ID
        }
    
    def start(self):
        """Start the feedback simulator in a separate thread"""
        if self.running:
            logger.warning("Feedback simulator is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"👥 Started feedback simulator (listening on {MOOD_TOPIC}, sending to {FEEDBACK_TOPIC})")
        
    def _run(self):
        """Main processing loop"""
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                song = message.value
                
                # Wait a short time to simulate listening
                time.sleep(random.uniform(1.0, 3.0))
                
                # Generate feedback
                feedback = self.generate_feedback(song)
                
                # Log the feedback
                emoji = "👍" if feedback["feedback_value"] > 0 else "👎"
                logger.info(f"{emoji} Feedback for {feedback['track_id']} ({song.get('mood', 'unknown')}): {feedback['action']}")
                
                # Send feedback to Kafka
                self.producer.send(FEEDBACK_TOPIC, feedback)
        except Exception as e:
            logger.error(f"Error in feedback simulator: {str(e)}")
            self.running = False
    
    def stop(self):
        """Stop the feedback simulator"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)
        self.consumer.close()
        self.producer.flush()
        self.producer.close()
        logger.info("Stopped feedback simulator")

class UserFeedbackCollector:
    def __init__(self, mysql_config=None):
        self.consumer = KafkaConsumer(
            FEEDBACK_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='feedback_collector'
        )
        
        # If MySQL config is provided, set up database connection
        self.mysql_config = mysql_config
        self.db_connection = None
        
        if mysql_config:
            self._connect_to_db()
    
    def _connect_to_db(self):
        """Connect to MySQL database (to be implemented)"""
        # This would use mysql.connector or similar to connect to the database
        # For now, we'll just log that we would store to DB
        logger.info("Would connect to MySQL database (not implemented yet)")
    
    def start_collecting(self):
        """Start collecting and storing user feedback"""
        logger.info(f"📊 Starting feedback collection from {FEEDBACK_TOPIC}")
        
        try:
            for message in self.consumer:
                feedback = message.value
                
                # Log the feedback
                logger.info(f"Collecting feedback: {feedback['action']} for {feedback['track_id']}")
                
                # Store to database (if configured)
                if self.mysql_config:
                    self._store_feedback(feedback)
                    
        except KeyboardInterrupt:
            logger.info("Feedback collection stopped by user")
        except Exception as e:
            logger.error(f"Error in feedback collection: {str(e)}")
        finally:
            self.consumer.close()
            
    def _store_feedback(self, feedback):
        """Store feedback to MySQL database"""
        # This would insert the feedback into a database
        # For now, we'll just log it
        logger.info(f"Would store to database: {feedback}")

def run_simulator():
    """Run the feedback simulator"""
    simulator = FeedbackSimulator()
    
    try:
        simulator.start()
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Simulator stopped by user")
    finally:
        simulator.stop()

def run_collector():
    """Run the feedback collector"""
    collector = UserFeedbackCollector()
    collector.start_collecting()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python user_feedback.py [simulator|collector]")
        sys.exit(1)
        
    mode = sys.argv[1].lower()
    
    if mode == "simulator":
        run_simulator()
    elif mode == "collector":
        run_collector()
    else:
        print(f"Unknown mode: {mode}. Use 'simulator' or 'collector'")
        sys.exit(1)