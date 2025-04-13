import mysql.connector
from mysql.connector import Error, pooling
import logging
import json
from kafka import KafkaConsumer
import time
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SONGS_TOPIC = "mood_classified"
FEEDBACK_TOPIC = "user_feedback"
PLAYLISTS_TOPIC = "playlist_recommendations"

# MySQL configuration
MYSQL_CONFIG = {
    'host': 'your_sql_host',
    'database': 'music_mood_analyzer',
    'user': 'sql_user',
    'password': 'sql_pass',
    'port': 3306,
    'autocommit': True,
    'connect_timeout': 30,
    'pool_name': 'mood_pool',
    'pool_size': 5,
    'pool_reset_session': True
}

class MySQLConnector:
    def __init__(self, config=None):
        self.config = config or MYSQL_CONFIG
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:
            self.connection_pool = pooling.MySQLConnectionPool(**self.config)
            logger.info("MySQL connection pool initialized")
        except Error as e:
            logger.error(f"Error initializing connection pool: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool with retry logic"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                connection = self.connection_pool.get_connection()
                if connection.is_connected():
                    connection.ping(reconnect=True, attempts=3, delay=1)
                    return connection
            except Error as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                logger.error("Max connection attempts reached")
                raise

    def execute_query(self, query, params=None, max_retries=3):
        """Execute a query with automatic retry and connection handling"""
        connection = None
        cursor = None
        
        for attempt in range(max_retries):
            try:
                connection = self.get_connection()
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                connection.commit()
                return True
            except Error as e:
                logger.error(f"Query attempt {attempt + 1} failed: {e}")
                if "Lost connection" in str(e) or "MySQL server has gone away" in str(e):
                    self._initialize_pool()  # Reinitialize pool if connection lost
                if attempt == max_retries - 1:
                    logger.error("Max query attempts reached")
                    return False
                time.sleep(1)
            finally:
                if cursor:
                    cursor.close()
                if connection:
                    connection.close()

    def create_tables(self):
        """Create database tables if they don't exist"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS songs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                track VARCHAR(255) NOT NULL,
                artist VARCHAR(255) NOT NULL,
                album VARCHAR(255),
                mood VARCHAR(50),
                tempo FLOAT,
                energy FLOAT,
                valence FLOAT,
                acousticness FLOAT,
                danceability FLOAT,
                timestamp BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS feedback (
                id INT AUTO_INCREMENT PRIMARY KEY,
                track_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255),
                mood VARCHAR(50),
                action VARCHAR(50),
                feedback_value FLOAT,
                timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS playlist_recommendations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp BIGINT,
                window_size INT,
                recommendations JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        for query in queries:
            if not self.execute_query(query):
                logger.error("Failed to create tables")
                return False
        logger.info("Database tables created/verified successfully")
        return True

    def insert_song(self, song):
        """Insert a song into the database"""
        query = """
        INSERT INTO songs (track, artist, album, mood, tempo, energy, valence, 
                          acousticness, danceability, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        features = song.get('features', {})
        values = (
            song.get('track', 'Unknown'),
            song.get('artist', 'Unknown'),
            song.get('album', ''),
            song.get('mood', 'unknown'),
            features.get('tempo', 0.0),
            features.get('energy', 0.0),
            features.get('valence', 0.0),
            features.get('acousticness', 0.0),
            features.get('danceability', 0.0),
            song.get('timestamp', int(time.time()))
        )
        
        return self.execute_query(query, values)

    def insert_feedback(self, feedback):
        """Insert user feedback into the database"""
        query = """
        INSERT INTO feedback (track_id, user_id, mood, action, feedback_value, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        values = (
            feedback.get('track_id', 'Unknown'),
            feedback.get('user_id', 'unknown'),
            feedback.get('mood', 'unknown'),
            feedback.get('action', 'unknown'),
            feedback.get('feedback_value', 0.0),
            feedback.get('timestamp')
        )
        
        return self.execute_query(query, values)

    def insert_recommendation(self, recommendation):
        """Insert playlist recommendation into the database"""
        query = """
        INSERT INTO playlist_recommendations (timestamp, window_size, recommendations)
        VALUES (%s, %s, %s)
        """
        
        values = (
            recommendation.get('timestamp', int(time.time())),
            recommendation.get('window_size', 0),
            json.dumps(recommendation.get('recommendations', {}))
        )
        
        return self.execute_query(query, values)

class KafkaToMySQL:
    def __init__(self):
        self.db = MySQLConnector()
        self.running = False
        self.consumers = {}
        self.threads = []

    def _init_consumer(self, topic, group_id):
        """Initialize a Kafka consumer"""
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=group_id,
            consumer_timeout_ms=1000
        )

    def start(self):
        """Start the Kafka to MySQL service"""
        if not self.db.create_tables():
            logger.error("Failed to set up database. Exiting.")
            return

        self.running = True
        
        # Initialize consumers
        self.consumers = {
            'songs': self._init_consumer(SONGS_TOPIC, 'mysql_connector_songs'),
            'feedback': self._init_consumer(FEEDBACK_TOPIC, 'mysql_connector_feedback'),
            'playlists': self._init_consumer(PLAYLISTS_TOPIC, 'mysql_connector_playlists')
        }

        # Start consumer threads
        self.threads = [
            threading.Thread(target=self._consume_songs),
            threading.Thread(target=self._consume_feedback),
            threading.Thread(target=self._consume_playlists)
        ]

        for thread in self.threads:
            thread.daemon = True
            thread.start()

        logger.info("🗄️ Started Kafka to MySQL service")

        try:
            while self.running:
                time.sleep(1)
                # Check if any threads died
                if any(not thread.is_alive() for thread in self.threads):
                    logger.error("One or more consumer threads died. Restarting...")
                    self.stop()
                    self.start()
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        finally:
            self.stop()

    def stop(self):
        """Stop the service"""
        self.running = False
        for consumer in self.consumers.values():
            consumer.close()
        for thread in self.threads:
            thread.join(timeout=5.0)
        logger.info("Kafka to MySQL service stopped")

    def _consume_songs(self):
        """Consume songs from Kafka and store them in MySQL"""
        while self.running:
            try:
                for message in self.consumers['songs']:
                    if not self.running:
                        break
                    song = message.value
                    if self.db.insert_song(song):
                        logger.info(f"Stored song: {song.get('track')} by {song.get('artist')}")
            except Exception as e:
                logger.error(f"Error in songs consumer: {str(e)}")
                time.sleep(5)

    def _consume_feedback(self):
        """Consume feedback from Kafka and store it in MySQL"""
        while self.running:
            try:
                for message in self.consumers['feedback']:
                    if not self.running:
                        break
                    feedback = message.value
                    if self.db.insert_feedback(feedback):
                        logger.info(f"Stored feedback: {feedback.get('track_id')} - {feedback.get('action')}")
            except Exception as e:
                logger.error(f"Error in feedback consumer: {str(e)}")
                time.sleep(5)

    def _consume_playlists(self):
        """Consume playlist recommendations from Kafka"""
        while self.running:
            try:
                for message in self.consumers['playlists']:
                    if not self.running:
                        break
                    recommendation = message.value
                    if self.db.insert_recommendation(recommendation):
                        logger.info("Stored playlist recommendation")
            except Exception as e:
                logger.error(f"Error in playlists consumer: {str(e)}")
                time.sleep(5)

def main():
    """Run the Kafka to MySQL service"""
    service = KafkaToMySQL()
    try:
        service.start()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        service.stop()

if __name__ == "__main__":
    main()