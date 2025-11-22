## Real Time Stock Analytics Pipeline Demo

https://github.com/user-attachments/assets/b200229b-f8b4-405b-996e-276f12cbc150

## Purpose

Personal practice project to build a real-time stock analytics pipeline using **Full-Stack Python** and **Apache Kafka**, **Apache Spark**, and **Finnhub for stock market data**.

## Getting Started

1. Run `source venv/bin/activate` in terminal (to start with a virtial env for python)
2. Run `pip install -r requirements.txt` to install the required libraries
3. Copy the `.env.example` file into a `.env` file and add your finnhub api key (you will have to create a new key), and your psql database credentials
4. Run `brew install openjdk@11` to install the java sdk (required for Spark)
5. Run `docker compose up` to start zookeeper, kafka, and postgresql
6. You will have to create a database with a table called stock_aggregates (incoming supabase soon)
7. Once your database is ready and your stock_aggregates table is ready run the three files in separated terminals `python kafka_stock_producer.py` first, and then `python spark_streaming_app.py` in another terminal
8. Finally run `source venv/bin/activate`, `pip install -r requirements.txt` and `python app.py` for dashboard frontend in a third terminal

## Flow Diagram

```text
        +-----------------+
        | Stock Producer  |
        | (kafka_stock_   |
        |  producer.py)   |
        +--------+--------+
                 |
                 v
        +-----------------+
        |      Kafka      |
        | (stock-market-  |
        |  data topic)    |
        +--------+--------+
                 |
                 v
        +-------------------------+
        |     Spark Streaming     |
        | (spark_streaming_app.py)|
        +-----+-----------+------+
              |           |
              |           v
              |    +----------------+
              |    | Console Output |
              |    | (debug/logs)   |
              |    +----------------+
              v
        +----------------+
        |   Postgres     |
        |  (stocks DB)   |
        +--------+-------+
                 |
                 v
        +----------------+
        |   Dashboard    |
        | (Flask)
        +----------------+
```
In short, the Stock Producer (using Kafka) is looking for updates from the stock market api every 60 seconds for these top tech stock symbols ('AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX'), the rate limit being 60 api calls per second and max 30 api calls a second.

These stock updates are then streamed as topics that are then consumed via the Apache Spark application (spark_streaming_app.py). This Spark app is tapping into the kafka stream and aggregating the stock market results into metrics that are saved into a PSQL database for persistent storage.

A light-weight frontend using flask is then taking this saved aggregated stock market data and polling the database for new entries to get a live feed of persisted stock market metrics.
