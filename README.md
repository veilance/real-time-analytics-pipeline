# real-time-analytics-pipeline

## Real Time Stock Analytics Pipeline Demo

https://github.com/user-attachments/assets/b200229b-f8b4-405b-996e-276f12cbc150

## Purpose

Personal practice project to build a real-time stock analytics pipeline.

## Getting Started

1. Run `source venv/bin/activate` in terminal (to start with a virtial env for python)
2. Run `pip install -r requirements.txt` to install the required libraries
3. Copy the `.env.example` file into a `.env` file and add your finnhub api key (you will have to create a new key), and your psql database credentials
4. You will have to create a database with a table called stock_aggregates
5. Once your database is ready and your stock_aggregates table is ready run the three files in separated terminals (kafka_stock_producer.py first), then (spark_streaming_app.py) and finally (app.py) for dashboard frontend

## Technology Used
Full-Stack Python and Apache Kafka, Apache Spark, and Finnhub for stock market data.
