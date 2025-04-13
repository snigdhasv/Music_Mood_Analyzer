from kafka import KafkaConsumer, KafkaProducer
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "song_stream"
OUTPUT_TOPIC = "mood_classified"

def classify_mood(features):
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




def process_songs():
    # Initialize Kafka consumer and producer
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    logger.info(f"🎧 Starting mood classification service (input: {INPUT_TOPIC}, output: {OUTPUT_TOPIC})")
    
    try:
        for message in consumer:
            song = message.value

            # Validate message
            if 'features' not in song:
                logger.warning(f"⚠️ No features found for: {song.get('track', 'Unknown')}")
                continue

            # Classify and log mood
            mood = classify_mood(song['features'])
            song['mood'] = mood
            logger.info(f"🎼 {song['track']} by {song['artist']} ➝ mood: {mood}")

            # Produce to output topic
            producer.send(OUTPUT_TOPIC, song)
            
    except KeyboardInterrupt:
        logger.info("🛑 Service stopped by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
    finally:
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == "__main__":
    process_songs()
