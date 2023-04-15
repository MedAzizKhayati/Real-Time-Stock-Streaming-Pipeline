import atexit
import logging
import json
import time
import sys
import finnhub
from apscheduler.schedulers.background import BackgroundScheduler

from flask import (
    Flask,
    request,
    jsonify
)

from kafka import KafkaProducer
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError
)

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format, stream=sys.stdout)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.INFO)

app = Flask(__name__)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
topic_name = app.config['CONFIG_KAFKA_TOPIC']
API_KEY = app.config['CONFIG_FINNHUB_API_KEY']

producer = KafkaProducer(
    bootstrap_servers=kafka_broker
)

schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

symbols = set()
finnhub_client = finnhub.Client(api_key=API_KEY)

def getQuotes(symbol):
    return finnhub_client.quote(symbol)

def shutdown_hook():
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)
    try:
        logger.info('shutdown scheduler')
        schedule.shutdown()
    except Exception as e:
        logger.warn('Failed to shutdown scheduler, caused by: %s', e.message)


def fetch_price(symbol):
    """
    helper function to retrieve stock data and send it to kafka
    :param symbol: symbol of the stock
    :return: None
    """
    symbol = str(symbol)
    logger.debug('Start to fetch stock price for %s', symbol)
    try:
        payload = getQuotes(symbol)
        logger.debug('Retrieved stock info %s', payload['c'])
        producer.send(topic=topic_name, value=json.dumps(payload).encode(), timestamp_ms=int(time.time()))
        logger.info('Sent stock price for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warning('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error.message))
    except Exception as e:
        logger.warning('Failed to fetch stock price for %s caused by: %s', symbol, type(e))


@app.route('/<symbol>', methods=['POST'])
def add_stock(symbol: str):
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol in symbols:
        pass
    else:
        symbols.add(symbol)
        logger.info('Add stock retrieve job %s' % symbol)
        schedule.add_job(fetch_price, 'interval', [symbol], seconds=1, id=symbol)
    return jsonify(results=list(symbols)), 200


@app.route('/<symbol>', methods=['DELETE'])
def del_stock(symbol):
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol not in symbols:
        pass
    else:
        symbols.remove(symbol)
        schedule.remove_job(symbol)
    return jsonify(results=list(symbols)), 200

if __name__ == '__main__':
    atexit.register(shutdown_hook)
    app.run(host='0.0.0.0', port=app.config['CONFIG_APPLICATION_PORT'])
