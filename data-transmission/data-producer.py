# @Author: yufeiz
# @Date:   2018-11-01T19:53:47-07:00
# @Last modified by:   yufeiz
# @Last modified time: 2018-11-02T19:30:43-07:00



import argparse
import atexit
import logging
import requests
import schedule
import time
import json
import websocket

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)


API_BASE = 'https://api-public.sandbox.pro.coinbase.com'
WEBSOCKET_FEED = 'wss://ws-feed.pro.coinbase.com'

def check_symbol(symbol):
    """
    Helper method to check if the symbol is supported in coinbase API.
    """
    logger.debug('Checking symbol.')
    try:
        response = requests.get(API_BASE + '/products')
        product_ids = [product['id'] for product in response.json()]
        if symbol not in product_ids:
            logger.warn('symbol %s not supported. The list of supported symbols: %s', symbol, product_ids)
            exit()

    except Exception as e:
        logger.warn('Failed to fetch products due to %s', e.message)

def on_open(ws, symbol) :
    logger.debug('On open.')
    sub_msg = {'type':'subscribe',
                'product_ids':[str(symbol)],
                'channels': ['ticker']}
    ws.send(json.dumps(sub_msg))

def fetch_price(response, symbol, producer, topic_name):
    """
    Helper method to retrieve data from coinbase API and send it to kafka.
    """
    logger.debug('Start to fetch price for %s', symbol)
    try:
        data = json.loads(response)
        if data['type'] != 'ticker' :
            return
        price = data['price']

        timestamp = time.time()
        payload = {'Symbol':str(symbol),
                   'LastTradePrice':str(price),
                   'Timestamp':str(timestamp)}

        logger.debug('Retrieved %s info %s', symbol, payload)
        producer.send(topic=topic_name, value=json.dumps(payload).encode('utf-8'))
        logger.debug('Sent price for %s to Kafka.', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send message to kafka due to %s', timeout_error.message)
    except Exception as e:
        logger.warn('Failed to fetch price due to: %s', e.message)


def shutdown_hook(producer, ws):
    """
    Helper method to close kafka connections at exit.
    """
    try:
        ws.close()
        producer.flush(10)
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka due to %s', kafka_error.message)
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to clsoe kafka connection due to %s', e.message)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol you want to pull.')
    parser.add_argument('topic_name', help='the kafka topic push to.')
    parser.add_argument('kafka_broker', help='the location of the kafka broker.')

    # Parse arguments.
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # Check if the symbol is supported.
    check_symbol(symbol)

    # Instantiate a simple kafka producer.
    producer = KafkaProducer(bootstrap_servers=kafka_broker)


    # Instantiate a websocket connection
    ws = websocket.WebSocketApp(WEBSOCKET_FEED, on_message=lambda ws, message: fetch_price(message, symbol, producer, topic_name),
                            on_open=lambda ws : on_open(ws, symbol))
    ws.run_forever()

    # Setup proper shutdown hook.
    atexit.register(shutdown_hook, producer, ws)
