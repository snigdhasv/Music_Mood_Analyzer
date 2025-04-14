from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

# Define schema for input
schema = StructType().add("track", StringType()) \
                     .add("artist", StringType()) \
                     .add("features", StructType()
                          .add("valence", FloatType())
                          .add("energy", FloatType())
                          .add("tempo", FloatType())
                          .add("acousticness", FloatType())
                          .add("danceability", FloatType()))

# Spark session
spark = SparkSession.builder \
    .appName("SparkMusicMoodClassifier") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "song_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka value (assumes it's JSON)
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data"))
songs = json_df.select("data.*")

# Define a simple UDF to classify mood
from pyspark.sql.functions import udf

def classify_mood(valence, energy, tempo, acousticness, danceability):
    if valence < 0.35 and energy < 0.5 and tempo < 100:
        return "sad"
    if acousticness > 0.5 and energy < 0.6 and tempo < 110:
        return "calm"
    if valence >= 0.5 and danceability > 0.5:
        return "happy"
    if energy > 0.6 and tempo > 115 and danceability > 0.5:
        return "energetic"
    if valence < 0.4 and energy > 0.6:
        return "dark"
    return "neutral"

mood_udf = udf(classify_mood, StringType())

# Apply UDF
classified = songs.withColumn("mood", mood_udf(
    col("features.valence"),
    col("features.energy"),
    col("features.tempo"),
    col("features.acousticness"),
    col("features.danceability")
))

# Write to Kafka or console
query = classified.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "mood_classified") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

query.awaitTermination()
