# Real-Time Music Mood Analyzer System

This project implements a complete real-time music mood analysis and recommendation system. It analyzes audio features of songs, classifies their mood, processes user feedback, and provides playlist recommendations based on user preferences.

## System Components

### 1. **Streaming Pipeline**
- **`stream_music.py`**: Simulates music streaming and extracts audio features.
- **`mood_classifier.py`**: Classifies song mood in real-time using Kafka.
- **`user_feedback.py`**: Simulates and collects listener feedback.
- **`spark_streaming_processor.py`**: Processes real-time music mood analysis using Apache Spark Streaming and Kafka, handling mood classification and feature extraction.

### 2. **Data Processing**
- **`mysql_connector.py`**: Manages database connections and Kafka-to-MySQL streaming.
- **`playlist_manager.py`**: Generates dynamic playlist recommendations.
- **`batch_processor.py`**: Handles batch processing of historical data.

### 3. Visualization
- `dashboard.py`: Interactive Plotly Dash dashboard showing real-time analytics of mood classification and recommendations.

## Technology Stack 🛠️

| Component          | Technology/Tool       |
|--------------------|-----------------------|
| Streaming          | Apache Kafka          |
| Database           | MySQL                 |
| Real-time Analysis | Python (Librosa)      |
| Batch Processing   | Python (Pandas)       |
| Dashboard          | Plotly Dash           |
| Audio Processing   | Librosa               |

## Setup Instructions

### 1. Install Dependencies

```bash
pip install mysql-connector-python dash plotly pandas numpy kafka-python librosa yt-dlp
```

### 2. Set Up MySQL Database

Create a MySQL database and tables using the following SQL commands:

```sql
CREATE DATABASE music_mood_analyzer;
USE music_mood_analyzer;
```

### 3. Configure Database Connection

Create a database named `music_mood_analyzer` and configure the credentials in:

- `mysql_connector.py`
- `dashboard.py`
- `batch_processor.py`

```python
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
```

### 4. Configure Kafka

Create Kafka topics for streaming and feedback handling:

```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic playlist_recommendations
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic mood_classified
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic song_stream
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic user_feedback
```

### 5. Get LastFM API
Login to LastFM for developers, get an API key, and update `stream_music.py` with your credentials:

```python
LASTFM_API_KEY = "your_api_key"
LASTFM_USER = "your_username"
```

## Usage
1. **Start Kafka** (in separate terminal):
```bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
kafka-server-start.sh config/server.properties
```

2. **Run the streaming pipeline** (in separate terminals):
```bash
# Simulate music stream
python stream_music.py

# Mood classification service-using kafka (run this if you want kafka instead of spark to classify mood)
python mood_classifier.py

# Feedback simulator
python user_feedback.py

# Database connector
python mysql_connector.py

# Playlist manager
python playlist_manager.py
```

3. *Run the Spark Streaming Pipeline* (in a separate terminal)
```bash
# Start Spark Streaming to process music data (mood classification using spark streaming)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark_streaming_processor.py
```


4. **Run dashboard**:
```bash
python dashboard.py
```
Access the dashboard at: `http://localhost:8051`

5. **Run Batch Processing**:
```bash
python batch_processor.py --classify  # Run batch classification
python batch_processor.py --analyze   # Generate recommendations
python batch_processor.py --compare   # Compare real-time vs batch
```

## Understanding Mood Classification

The system classifies songs into the following moods based on audio features:

- **Happy**: High valence (positivity) and danceability
- **Energetic**: High energy, fast tempo, and good danceability
- **Calm**: Acoustic, soft, and slower tempo
- **Sad**: Low valence, low energy, and slower tempo
- **Dark**: Low valence but high energy
- **Neutral**: Songs that don't fit clearly into other categories

## Project Structure

```
music-mood-analyzer/
├── batch_processor.py                  # Batch processing and analysis
├── dashboard.py                        # Visualization dashboard
├── mood_classifier.py                  # Real-time mood classification
├── mysql_connector.py                  # Database integration
├── playlist_manager.py                 # Dynamic playlist management
├── spark_streaming_processor.py        # Real-time mood classification using Spark
├── stream_music.py                     # Music stream simulator
├── user_feedback.py                    # Feedback simulator/collector
├── requirements.txt                    # Python dependencies
└── README.md                           # This file
```

---