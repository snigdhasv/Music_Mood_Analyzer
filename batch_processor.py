import mysql.connector
from mysql.connector import Error
import logging
import argparse
import time
import json
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

class BatchProcessor:
    def __init__(self, config=None):
        self.config = config or MYSQL_CONFIG
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Connect to MySQL database"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                logger.info(f"Connected to MySQL database: {self.config['database']}")
                return True
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return False
    
    def close(self):
        """Close the database connection"""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()
            logger.info("MySQL connection closed")
    
    def classify_mood_batch(self, start_time=None, end_time=None):
        """
        Re-classify songs in batch mode using the same algorithm
        as the real-time classifier
        """
        try:
            # Build query conditions for time range if provided
            conditions = []
            params = []
            
            if start_time:
                conditions.append("timestamp >= %s")
                params.append(start_time)
            
            if end_time:
                conditions.append("timestamp <= %s")
                params.append(end_time)
            
            # Build the complete query
            query = "SELECT * FROM songs"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Execute the query
            self.cursor.execute(query, params)
            songs = self.cursor.fetchall()
            
            logger.info(f"Found {len(songs)} songs for batch processing")
            
            # Process each song
            updated_count = 0
            for song in songs:
                # Extract features
                features = {
                    'valence': song['valence'],
                    'energy': song['energy'],
                    'tempo': song['tempo'],
                    'acousticness': song['acousticness'],
                    'danceability': song['danceability']
                }
                
                # Classify mood using the same algorithm as real-time classifier
                mood = self._classify_mood(features)
                
                # Update the song if the mood classification changed
                if mood != song['mood']:
                    update_query = "UPDATE songs SET mood = %s WHERE id = %s"
                    self.cursor.execute(update_query, (mood, song['id']))
                    updated_count += 1
            
            # Commit the changes
            self.connection.commit()
            logger.info(f"Batch processing complete. Updated {updated_count} song classifications.")
            
            return updated_count
        
        except Error as e:
            logger.error(f"Error in batch classification: {e}")
            return 0
    
    def _classify_mood(self, features):
        """Same mood classification as in the real-time processor"""
        valence = features.get('valence', 0)
        energy = features.get('energy', 0)
        tempo = features.get('tempo', 0)
        acousticness = features.get('acousticness', 0)
        danceability = features.get('danceability', 0)

        # Sad: low valence, low energy
        if valence < 0.35 and energy < 0.5 and tempo < 100:
            return "sad"

        # Calm: soft, slow, acoustic
        if acousticness > 0.5 and energy < 0.6 and tempo < 110:
            return "calm"

        # Happy: high valence and danceability
        if valence >= 0.5 and danceability > 0.5:
            return "happy"

        # Energetic: fast and loud
        if energy > 0.6 and tempo > 115 and danceability > 0.5:
            return "energetic"

        # Dark: low valence + high energy
        if valence < 0.4 and energy > 0.6:
            return "dark"

        return "neutral"
    
    def analyze_feedback_by_mood(self, start_time=None, end_time=None):
        """Analyze user feedback by mood to determine optimal playlist composition"""
        try:
            # Build query conditions for time range if provided
            conditions = []
            params = []
            
            if start_time:
                conditions.append("timestamp >= %s")
                params.append(start_time)
            
            if end_time:
                conditions.append("timestamp <= %s")
                params.append(end_time)
            
            # Build the complete query
            query = "SELECT mood, action, feedback_value FROM feedback"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Execute the query
            self.cursor.execute(query, params)
            feedback_records = self.cursor.fetchall()
            
            logger.info(f"Analyzing {len(feedback_records)} feedback records")
            
            # Process feedback by mood
            mood_feedback = defaultdict(list)
            for record in feedback_records:
                mood = record['mood']
                feedback_value = record['feedback_value']
                
                mood_feedback[mood].append(feedback_value)
            
            # Calculate average feedback for each mood
            mood_scores = {}
            for mood, values in mood_feedback.items():
                if values:
                    avg_score = sum(values) / len(values)
                    mood_scores[mood] = avg_score
            
            # Sort moods by preference
            sorted_moods = sorted(mood_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Generate recommended distribution
            total_score = sum(abs(score) for _, score in sorted_moods)
            if total_score == 0:
                total_score = 1  # Avoid division by zero
                
            recommendations = {}
            for mood, score in sorted_moods:
                # Convert scores to positive weights
                adjusted_score = (score + 1) / 2  # Map from [-1,1] to [0,1]
                recommendations[mood] = max(0.1, adjusted_score)  # Ensure at least 10% for variety
            
            # Normalize to sum to 1.0
            total = sum(recommendations.values())
            if total > 0:
                recommendations = {mood: value / total for mood, value in recommendations.items()}
            
            return recommendations
        
        except Error as e:
            logger.error(f"Error in feedback analysis: {e}")
            return {}
    
    def compare_with_real_time(self, time_window=3600):
        """
        Compare batch processing results with real-time processing
        within a given time window (default: last hour)
        """
        try:
            # Get end time as the most recent song timestamp
            self.cursor.execute("SELECT MAX(timestamp) as max_time FROM songs")
            result = self.cursor.fetchone()
            end_time = result['max_time'] if result and result['max_time'] else int(time.time())
            
            # Set start time as one hour before end time
            start_time = end_time - time_window
            
            logger.info(f"Comparing real-time vs batch processing ({time_window} seconds window)")
            
            # Get real-time classifications
            query = """
            SELECT mood, COUNT(*) as count
            FROM songs
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY mood
            """
            self.cursor.execute(query, (start_time, end_time))
            real_time_counts = {row['mood']: row['count'] for row in self.cursor.fetchall()}
            
            # Get real-time recommendations
            query = """
            SELECT recommendations
            FROM playlist_recommendations
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp DESC
            LIMIT 1
            """
            self.cursor.execute(query, (start_time, end_time))
            real_time_rec = self.cursor.fetchone()
            
            real_time_recommendations = {}
            if real_time_rec and real_time_rec['recommendations']:
                try:
                    real_time_recommendations = json.loads(real_time_rec['recommendations'])
                except:
                    real_time_recommendations = {}
            
            # Run batch processing on the same time window
            self.classify_mood_batch(start_time, end_time)
            
            # Get batch processing classifications
            query = """
            SELECT mood, COUNT(*) as count
            FROM songs
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY mood
            """
            self.cursor.execute(query, (start_time, end_time))
            batch_counts = {row['mood']: row['count'] for row in self.cursor.fetchall()}
            
            # Get batch recommendations
            batch_recommendations = self.analyze_feedback_by_mood(start_time, end_time)
            
            # Compare results
            comparison = {
                'time_window': time_window,
                'start_time': start_time,
                'end_time': end_time,
                'mood_distribution': {
                    'real_time': real_time_counts,
                    'batch': batch_counts
                },
                'playlist_recommendations': {
                    'real_time': real_time_recommendations,
                    'batch': batch_recommendations
                }
            }
            
            # Calculate consistency score (how similar are the classifications)
            all_moods = set(list(real_time_counts.keys()) + list(batch_counts.keys()))
            total_songs = sum(real_time_counts.values())
            
            if total_songs > 0:
                differences = 0
                for mood in all_moods:
                    real_count = real_time_counts.get(mood, 0)
                    batch_count = batch_counts.get(mood, 0)
                    differences += abs(real_count - batch_count)
                
                consistency = 1.0 - (differences / (2 * total_songs))
                comparison['consistency_score'] = consistency
            else:
                comparison['consistency_score'] = 0.0
            
            return comparison
        
        except Error as e:
            logger.error(f"Error comparing processing methods: {e}")
            return {}

def main():
    parser = argparse.ArgumentParser(description='Batch processor for Music Mood Analyzer')
    parser.add_argument('--classify', action='store_true', help='Run batch classification on all songs')
    parser.add_argument('--analyze', action='store_true', help='Analyze feedback and generate recommendations')
    parser.add_argument('--compare', action='store_true', help='Compare real-time vs batch processing')
    parser.add_argument('--window', type=int, default=3600, help='Time window for comparison (seconds)')
    
    args = parser.parse_args()
    
    processor = BatchProcessor()
    
    if not processor.connect():
        return
    
    try:
        if args.classify:
            processor.classify_mood_batch()
        
        if args.analyze:
            recommendations = processor.analyze_feedback_by_mood()
            print("\n🎵 Recommended Playlist Distribution:")
            for mood, weight in sorted(recommendations.items(), key=lambda x: x[1], reverse=True):
                print(f"  {mood}: {weight*100:.1f}%")
        
        if args.compare:
            comparison = processor.compare_with_real_time(args.window)
            
            print("\n📊 Comparison Results:")
            print(f"Time window: {comparison.get('time_window', 0)} seconds")
            print(f"Consistency score: {comparison.get('consistency_score', 0)*100:.1f}%")
            
            print("\nMood Distribution:")
            real_time = comparison.get('mood_distribution', {}).get('real_time', {})
            batch = comparison.get('mood_distribution', {}).get('batch', {})
            
            all_moods = sorted(set(list(real_time.keys()) + list(batch.keys())))
            for mood in all_moods:
                print(f"  {mood}: Real-time={real_time.get(mood, 0)}, Batch={batch.get(mood, 0)}")
            
            print("\nPlaylist Recommendations:")
            real_time_rec = comparison.get('playlist_recommendations', {}).get('real_time', {})
            batch_rec = comparison.get('playlist_recommendations', {}).get('batch', {})
            
            all_rec_moods = sorted(set(list(real_time_rec.keys()) + list(batch_rec.keys())))
            for mood in all_rec_moods:
                rt_pct = real_time_rec.get(mood, 0) * 100
                b_pct = batch_rec.get(mood, 0) * 100
                print(f"  {mood}: Real-time={rt_pct:.1f}%, Batch={b_pct:.1f}%")
        
        # If no arguments, show help
        if not (args.classify or args.analyze or args.compare):
            parser.print_help()
    
    finally:
        processor.close()

if __name__ == "__main__":
    main()