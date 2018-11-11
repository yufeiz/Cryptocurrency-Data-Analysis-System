import argparse
import atexit
import happybase
import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError


logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)


def shutdown_hook(kafka_consumer, hbase_connection):
    """
    A shutdown hook to be called before the shutdown.
    """
    try:
        logger.info('Closing kafka consumer.')
        kafka_consumer.close()
        logger.info('Kafka consumer closed.')
        logger.info('Closing hbase connection.')
        hbase_connection.close()
        logger.info('Hbase connection closed.')
    except KafkaError as kafka_error:
        logger.warn('Failed to close kafka consumer, caused by: %s', kafka_error.message)
    except Exception as e:
        logger.warn('Failed to close open connections, caused by: %s', e)
    finally:
        logger.info('Existing program.')


def persist_data(data, hbase_connection, data_table):
    """
    Persist data into hbase.
    """
    try:
        logger.debug('Start to persist data to hbase %s', data)
        parsed = json.loads(data)
        symbol = parsed.get('Symbol')
        price = float(parsed.get('LastTradePrice'))
        timestamp = parsed.get('Timestamp')

        table = hbase_connection.table(data_table)
        row_key = "%s-%s" % (symbol, timestamp)
        logger.debug('Storing values with row key %s', row_key)
        table.put(row_key, {'family:symbol' : str(symbol),
                            'family:trade_time' : str(timestamp),
                            'family:trade_price' : str(price)})
        logger.info('Persisted data to hbase for symbol: %s, price: %f, timestamp: %s',
            symbol, price, timestamp)

    except Exception as e:
        logger.error('Failed to persist data to hbase for %s', e)

if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to subscribe from.')
    parser.add_argument('kafka_broker', help='the location of the kafka broker.')
    parser.add_argument('data_table', help='the data table to use in hbase.')
    parser.add_argument('hbase_host', help='the host name of hbase.')

    # Parse arguments.
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    data_table = args.data_table
    hbase_host = args.hbase_host

    # Initiate a simple kafka consumer.
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # Initiate a hbase connection.
    hbase_connection = happybase.Connection(hbase_host)

    # Create table if not exists.
    if data_table not in hbase_connection.tables():
        hbase_connection.create_table(data_table, { 'family':dict() })

    # Setup proper shutdown hook.
    atexit.register(shutdown_hook, consumer, hbase_connection)

    # Consume kafka and write to hbase.
    for msg in consumer:
        persist_data(msg.value, hbase_connection, data_table)
