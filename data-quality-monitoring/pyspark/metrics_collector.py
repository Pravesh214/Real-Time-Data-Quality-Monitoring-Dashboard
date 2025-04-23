
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json

spark = SparkSession.builder.appName("QualityCheck").getOrCreate()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def compute_metrics(df):
    null_percentage = df.filter(df["column"].isNull()).count() / df.count()
    metrics = {
        "null_percentage": null_percentage,
        "record_count": df.count()
    }
    producer.send("data-quality-metrics", metrics)

df = spark.read.csv("sample_data.csv", header=True)
compute_metrics(df)
