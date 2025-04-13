import json
from kafka import KafkaConsumer
import time
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def monitor_kafka_topic(topic_name="song_stream", pretty_print=True, timeout=None):
    """
    Monitor a Kafka topic and print the messages being received.
    
    Args:
        topic_name: Name of the Kafka topic to monitor
        pretty_print: Whether to format the JSON output for readability
        timeout: Number of seconds to monitor (None for indefinite)
    """
    logger.info(f"Starting Kafka consumer for topic: {topic_name}")
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',  # Start reading from the latest messages
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000  # Allows the consumer to timeout periodically for clean exit
    )
    
    # Display information
    logger.info("Waiting for messages... (Press Ctrl+C to exit)")
    
    # Variables for tracking
    start_time = time.time()
    message_count = 0
    
    try:
        while True:
            # Check timeout if specified
            if timeout and (time.time() - start_time > timeout):
                logger.info(f"Monitoring timeout reached ({timeout} seconds)")
                break
                
            # Poll for messages
            for message in consumer:
                message_count += 1
                logger.info(f"\n--- New message received (#{message_count}) ---")
                logger.info(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
                logger.info(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message.timestamp/1000))}")
                
                # Print message content
                if pretty_print:
                    # Format JSON for readability
                    formatted_json = json.dumps(message.value, indent=2)
                    logger.info(f"Content:\n{formatted_json}")
                else:
                    # Print raw data
                    logger.info(f"Content: {message.value}")
                
                logger.info("-----------------------------------\n")
            
            # Sleep briefly to avoid consuming too much CPU
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
    finally:
        # Close the consumer
        consumer.close()
        logger.info(f"Consumed {message_count} messages from topic '{topic_name}'")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Monitor messages on a Kafka topic")
    parser.add_argument("-t", "--topic", default="song_stream", help="Kafka topic to monitor")
    parser.add_argument("-r", "--raw", action="store_true", help="Display raw message format (not pretty JSON)")
    parser.add_argument("-d", "--duration", type=int, help="Duration in seconds to monitor (default: indefinite)")
    
    args = parser.parse_args()
    
    # Start monitoring
    monitor_kafka_topic(
        topic_name=args.topic,
        pretty_print=not args.raw,
        timeout=args.duration
    )