"""
Apache Spark Streaming Application for Real-time Stock Data Processing

This application consumes stock data from Kafka and performs real-time analytics:
- Moving averages (5-minute, 15-minute windows)
- Price volatility tracking
- Volume analysis
- Top movers identification
- Aggregated metrics by symbol

Requirements:
pip install pyspark kafka-python
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum, max, min, count, 
    stddev, expr, to_timestamp, current_timestamp, lit,
    first, last, unix_timestamp, when, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, LongType
)

import os
import psycopg2
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockStreamProcessor:
    def __init__(self, kafka_servers, kafka_topic, checkpoint_dir='/tmp/spark-checkpoint'):
        """
        Initialize Spark Streaming application
        
        Args:
            kafka_servers: Kafka bootstrap servers (e.g., 'localhost:9092')
            kafka_topic: Kafka topic to consume from
            checkpoint_dir: Directory for Spark checkpointing
        """
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_dir = checkpoint_dir
        
        # Create Spark Session
        self.spark = (
            SparkSession.builder
                .appName("StockStreamProcessor")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
                .getOrCreate()
        )
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark Session created: {self.spark.version}")
        logger.info(f"Reading from Kafka: {kafka_servers}/{kafka_topic}")
    
    def define_schema(self):
        """Define schema for incoming stock data"""
        return StructType([
            StructField("symbol", StringType(), True),
            StructField("current_price", DoubleType(), True),
            StructField("price", DoubleType(), True),  # Fallback field
            StructField("change", DoubleType(), True),
            StructField("percent_change", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("previous_close", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("source", StringType(), True)
        ])
    
    def write_to_postgres(self, df, batch_id):
        logger.info(f"Writing batch {batch_id} to Supabase Postgres...")
        conn = psycopg2.connect(
            host="localhost",
            port=5432,          # default Postgres port
            database="stocks",
            user="myuser",
            password="test123"
        )
        cursor = conn.cursor()

        for row in df.collect():
            cursor.execute("""
                INSERT INTO stock_aggregates (
                    symbol, window_start, window_end,
                    avg_price, max_price, min_price,
                    tick_count, price_stddev,
                    period_open, period_close, period_change_pct
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["symbol"],
                row["period_start"],
                row["period_end"],
                row["avg_price"],
                row["max_price"],
                row["min_price"],
                row["tick_count"],
                row["price_stddev"],
                row["period_open"],
                row["period_close"],
                row["period_change_pct"],
            ))

        conn.commit()
        cursor.close()
        conn.close()
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        schema = self.define_schema()
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
            .select(from_json(col("json_data"), schema).alias("data")) \
            .select("data.*")
        
        # Unify price field (handle both current_price and price)
        unified_df = parsed_df.withColumn(
            "price",
            when(col("current_price").isNotNull(), col("current_price"))
            .otherwise(col("price"))
        )
        
        # Convert timestamp string to timestamp type
        final_df = unified_df.withColumn(
            "event_time",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
        ).withColumn(
            "event_time",
            when(col("event_time").isNull(), current_timestamp())
            .otherwise(col("event_time"))
        )
        
        logger.info("Kafka stream configured successfully")
        return final_df
    
    def compute_moving_averages(self, df):
        """
        Compute moving averages over different time windows
        - 5-minute window
        - 15-minute window
        """
        # 5-minute moving average
        ma_5min = df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(
                window(col("event_time"), "5 minutes", "1 minute"),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("ma_5min"),
                count("price").alias("count_5min"),
                max("price").alias("high_5min"),
                min("price").alias("low_5min"),
                stddev("price").alias("volatility_5min"),
                first("price").alias("open_5min"),
                last("price").alias("close_5min")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("symbol"),
                col("ma_5min"),
                col("count_5min"),
                col("high_5min"),
                col("low_5min"),
                col("volatility_5min"),
                col("open_5min"),
                col("close_5min"),
                ((col("close_5min") - col("open_5min")) / col("open_5min") * 100).alias("change_pct_5min")
            )
        
        return ma_5min
    
    def detect_price_spikes(self, df):
        """
        Detect significant price movements (spikes/drops)
        Alert when price changes by more than 2% in a short window
        """
        spikes = df \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("symbol")
            ) \
            .agg(
                first("price").alias("start_price"),
                last("price").alias("end_price"),
                max("price").alias("max_price"),
                min("price").alias("min_price"),
                count("*").alias("tick_count")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("symbol"),
                col("start_price"),
                col("end_price"),
                col("max_price"),
                col("min_price"),
                col("tick_count"),
                ((col("end_price") - col("start_price")) / col("start_price") * 100).alias("change_pct")
            ) \
            .filter(spark_abs(col("change_pct")) > 2.0)
        
        return spikes
    
    def compute_aggregated_metrics(self, df):
        """
        Compute real-time aggregated metrics per symbol
        Updates every 30 seconds
        """
        metrics = df \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window(col("event_time"), "30 seconds", "10 seconds"),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                max("price").alias("max_price"),
                min("price").alias("min_price"),
                count("price").alias("tick_count"),
                stddev("price").alias("price_stddev"),
                avg("percent_change").alias("avg_change_pct"),
                first("price").alias("period_open"),
                last("price").alias("period_close")
            ) \
            .select(
                col("window.start").alias("period_start"),
                col("window.end").alias("period_end"),
                col("symbol"),
                col("avg_price"),
                col("max_price"),
                col("min_price"),
                col("tick_count"),
                col("price_stddev"),
                col("avg_change_pct"),
                col("period_open"),
                col("period_close"),
                ((col("period_close") - col("period_open")) / col("period_open") * 100).alias("period_change_pct")
            )
        
        return metrics
    
    def identify_top_movers(self, df):
        """
        Identify top gainers and losers in real-time
        Based on 5-minute windows
        """
        movers = df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("symbol")
            ) \
            .agg(
                first("price").alias("start_price"),
                last("price").alias("current_price"),
                avg("percent_change").alias("avg_change")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("symbol"),
                col("start_price"),
                col("current_price"),
                ((col("current_price") - col("start_price")) / col("start_price") * 100).alias("change_pct"),
                col("avg_change")
            ) \
            .orderBy(col("change_pct").desc())
        
        return movers
    
    def write_to_console(self, df, query_name, output_mode="complete"):
        """Write streaming results to console"""
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "20") \
            .queryName(query_name) \
            .start()
        
        return query
    
    def write_to_memory(self, df, table_name, output_mode="complete"):
        """Write streaming results to in-memory table for SQL queries"""
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .format("memory") \
            .queryName(table_name) \
            .start()
        
        return query
    
    def write_to_kafka(self, df, output_topic, checkpoint_location):
        """Write processed results back to Kafka"""
        query = df \
            .selectExpr("CAST(symbol AS STRING) AS key", "to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("topic", output_topic) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode("update") \
            .start()
        
        return query
    
    def run_analytics_pipeline(self):
        """
        Run complete analytics pipeline with multiple queries
        """
        logger.info("Starting Spark Streaming Analytics Pipeline...")
        
        # Read streaming data
        stream_df = self.read_kafka_stream()
        
        # 1. Moving Averages
        ma_df = self.compute_moving_averages(stream_df)
        query_ma = self.write_to_console(ma_df, "moving_averages", "complete")
        
        # 2. Price Spike Detection
        spikes_df = self.detect_price_spikes(stream_df)
        query_spikes = self.write_to_console(spikes_df, "price_spikes", "append")
        
        # 3. Aggregated Metrics
        metrics_df = self.compute_aggregated_metrics(stream_df)
        # query_metrics = self.write_to_memory(metrics_df, "stock_metrics", "complete")
        query_metrics = metrics_df.writeStream \
            .foreachBatch(self.write_to_postgres) \
            .outputMode("update") \
            .option("checkpointLocation", f"{self.checkpoint_dir}/metrics") \
            .start()
        
        # 4. Top Movers
        movers_df = self.identify_top_movers(stream_df)
        query_movers = self.write_to_console(movers_df, "top_movers", "complete")
        
        logger.info("All streaming queries started successfully!")
        logger.info("Press Ctrl+C to stop...")
        
        # Keep queries running
        queries = [query_ma, query_spikes, query_metrics, query_movers]
        
        try:
            # Wait for termination
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping all queries...")
            for query in queries:
                query.stop()
            self.spark.stop()
            logger.info("Spark session stopped")
    
    def run_simple_aggregation(self):
        """
        Simpler version - just compute basic aggregations
        Good for getting started
        """
        logger.info("Starting Simple Aggregation Pipeline...")
        
        # Read streaming data
        stream_df = self.read_kafka_stream()
        
        # Simple aggregation: average price per symbol every 30 seconds
        aggregated = stream_df \
            .groupBy(
                window(col("event_time"), "30 seconds"),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                count("*").alias("count"),
                max("price").alias("max_price"),
                min("price").alias("min_price")
            )
        
        # Write to console
        query = aggregated \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        logger.info("Simple aggregation query started!")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping query...")
            query.stop()
            self.spark.stop()


# Example usage
if __name__ == '__main__':
    # Configuration
    KAFKA_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "stock-market-data"
    CHECKPOINT_DIR = "/tmp/spark-checkpoint"
    
    # Create processor
    processor = StockStreamProcessor(
        kafka_servers=KAFKA_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        checkpoint_dir=CHECKPOINT_DIR
    )
    
    # Run analytics pipeline
    # Option 1: Full analytics pipeline (multiple queries)
    processor.run_analytics_pipeline()
    
    # Option 2: Simple aggregation (single query - good for testing)
    # processor.run_simple_aggregation()
