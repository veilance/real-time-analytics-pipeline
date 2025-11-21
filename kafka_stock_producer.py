"""
Real-time Stock Data Producer using Apache Kafka and Finnhub API

This producer fetches real-time stock quotes from Finnhub
and publishes them to a Kafka topic for downstream processing.

Finnhub Free Tier: 60 API calls per minute
Get API key: https://finnhub.io/register

Requirements:
pip install kafka-python requests python-dotenv
"""

import json
import time
import requests
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import logging

load_dotenv()

FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')  # with default
TOPIC = os.getenv('KAFKA_TOPIC', 'stock-market-data')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataProducer:
    def __init__(self, bootstrap_servers, api_key, topic='stock-data'):
        """
        Initialize Kafka producer and Finnhub configuration
        
        Args:
            bootstrap_servers: Kafka broker address (e.g., 'localhost:9092')
            api_key: Finnhub API key
            topic: Kafka topic name
        """
        self.api_key = api_key
        self.topic = topic
        self.base_url = 'https://finnhub.io/api/v1'
        
        # Initialize Kafka producer with JSON serialization
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"Kafka producer initialized: {bootstrap_servers}")
        logger.info(f"Using Finnhub API (60 calls/minute free tier)")
    
    def fetch_quote(self, symbol):
        """
        Fetch real-time quote for a symbol using Finnhub
        
        API Doc: https://finnhub.io/docs/api/quote
        """
        params = {
            'symbol': symbol,
            'token': self.api_key
        }
        
        try:
            response = requests.get(
                f'{self.base_url}/quote',
                params=params,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            # Check if we got valid data
            if data.get('c') is not None:  # 'c' is current price
                return {
                    'symbol': symbol,
                    'current_price': data['c'],      # Current price
                    'change': data['d'],             # Change
                    'percent_change': data['dp'],    # Percent change
                    'high': data['h'],               # High price of the day
                    'low': data['l'],                # Low price of the day
                    'open': data['o'],               # Open price of the day
                    'previous_close': data['pc'],    # Previous close price
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': 'finnhub'
                }
            else:
                logger.warning(f"No data for {symbol}: {data}")
                return None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.error(f"Rate limit exceeded! Wait before making more requests")
            else:
                logger.error(f"HTTP error fetching quote for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return None
    
    # def fetch_company_profile(self, symbol):
    #     """
    #     Fetch company profile information
        
    #     API Doc: https://finnhub.io/docs/api/company-profile2
    #     """
    #     params = {
    #         'symbol': symbol,
    #         'token': self.api_key
    #     }
        
    #     try:
    #         response = requests.get(
    #             f'{self.base_url}/stock/profile2',
    #             params=params,
    #             timeout=10
    #         )
    #         response.raise_for_status()
    #         data = response.json()
            
    #         if data:
    #             return {
    #                 'symbol': symbol,
    #                 'company_name': data.get('name'),
    #                 'industry': data.get('finnhubIndustry'),
    #                 'market_cap': data.get('marketCapitalization'),
    #                 'country': data.get('country'),
    #                 'exchange': data.get('exchange'),
    #                 'ipo': data.get('ipo'),
    #                 'website': data.get('weburl'),
    #                 'timestamp': datetime.utcnow().isoformat()
    #             }
    #         return None
            
    #     except Exception as e:
    #         logger.error(f"Error fetching profile for {symbol}: {e}")
    #         return None
    
    def send_to_kafka(self, data, key=None):
        """Send data to Kafka topic"""
        try:
            future = self.producer.send(
                self.topic,
                key=key,
                value=data
            )
            
            # Block for 'synchronous' sends
            record_metadata = future.get(timeout=10)
            logger.info(
                f"✓ Sent {key}: ${data.get('current_price', 'N/A')} "
                f"[{data.get('percent_change', 0):+.2f}%] → "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def stream_quotes(self, symbols, interval_seconds=60, calls_per_minute=60):
        """
        Continuously stream stock quotes for multiple symbols
        
        Args:
            symbols: List of stock symbols (e.g., ['AAPL', 'GOOGL', 'MSFT'])
            interval_seconds: Seconds between full cycles (default: 60)
            calls_per_minute: Rate limit (default: 60 for free tier)
        """
        logger.info(f"Starting stream for {len(symbols)} symbols: {symbols}")
        logger.info(f"Rate limit: {calls_per_minute} calls/minute")
        logger.info(f"Cycle interval: {interval_seconds} seconds")
        
        # Calculate delay between API calls to respect rate limit
        min_delay = 60 / calls_per_minute  # seconds per call
        logger.info(f"Min delay between calls: {min_delay:.2f}s")
        
        call_count = 0
        minute_start = time.time()
        
        try:
            while True:
                cycle_start = time.time()
                
                for symbol in symbols:
                    # Check if we need to reset the minute counter
                    if time.time() - minute_start >= 60:
                        logger.info(f"Minute elapsed. Made {call_count} calls. Resetting counter.")
                        call_count = 0
                        minute_start = time.time()
                    
                    # Check if we're approaching rate limit
                    if call_count >= calls_per_minute - 1:
                        wait_time = 60 - (time.time() - minute_start)
                        if wait_time > 0:
                            logger.warning(f"Rate limit approaching. Waiting {wait_time:.1f}s...")
                            time.sleep(wait_time)
                            call_count = 0
                            minute_start = time.time()
                    
                    # Fetch and send quote data
                    quote = self.fetch_quote(symbol)
                    if quote:
                        self.send_to_kafka(quote, key=symbol)
                        call_count += 1
                    
                    # Respect rate limit
                    time.sleep(min_delay)
                
                cycle_duration = time.time() - cycle_start
                wait_time = max(0, interval_seconds - cycle_duration)
                
                if wait_time > 0:
                    logger.info(
                        f"Cycle complete in {cycle_duration:.1f}s. "
                        f"Waiting {wait_time:.1f}s before next cycle..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.info(f"Cycle took {cycle_duration:.1f}s (longer than interval)")
                
        except KeyboardInterrupt:
            logger.info("Stopping stream...")
        finally:
            self.close()
    
    def close(self):
        """Close Kafka producer connection"""
        logger.info("Flushing remaining messages...")
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")


# Example usage
if __name__ == '__main__':    
    # Symbols to track (US stocks - use exchange:symbol for international)
    # Free tier: 60 calls/minute = can check 60 stocks every minute!
    
    # Option 1: Small portfolio (check every minute)
    SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX']
    
    # Option 2: Larger portfolio (check every 5 minutes)
    # SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX',
    #            'JPM', 'V', 'WMT', 'DIS', 'PYPL', 'INTC', 'AMD', 'COIN']
    
    # Option 3: Full utilization (60 stocks every minute!)
    # SYMBOLS = [
    #     'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
    #     'JPM', 'V', 'MA', 'WMT', 'DIS', 'BAC', 'PFE', 'KO', 'PEP', 'CSCO',
    #     'INTC', 'AMD', 'CRM', 'ORCL', 'ADBE', 'TXN', 'QCOM', 'IBM', 'UBER',
    #     'SQ', 'SHOP', 'SNAP', 'PINS', 'SPOT', 'ZM', 'DOCU', 'TWLO', 'NET'
    # ]
    
    # Create producer instance
    producer = StockDataProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_key=FINNHUB_API_KEY,
        topic=TOPIC
    )
    
    # Start streaming
    # Mode 1: Standard (check every minute)
    producer.stream_quotes(SYMBOLS, interval_seconds=60, calls_per_minute=60)
    
    # Mode 2: Fast updates (every 5 seconds for active trading)
    # producer.stream_quotes_fast(SYMBOLS, refresh_seconds=5)