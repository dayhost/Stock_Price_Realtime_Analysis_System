import ConfigParser
from kafka import KafkaConsumer
import redis
import logging
import atexit
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def shutdown_hook(consumer):
    try:
        consumer.close()
        logger.info('Kafka consumer closed')
    except Exception as e:
        logger.error(e.message)


if __name__ == '__main__':
    path = os.getcwd()
    parser = ConfigParser.ConfigParser()
    parser.read(path+'/redis_config.ini')
    kafka_cluster = parser.get('kafka_config','cluster')
    kafka_topic = parser.get('kafka_config','topic')
    redis_host = parser.get('redis_config','host')
    redis_port = parser.get('redis_config','port')
    redis_channel = parser.get('redis_config','channel')

    consumer = KafkaConsumer(
        kafka_topic,
        group_id='mygroup',
        bootstrap_servers=kafka_cluster)

    redis_client = redis.StrictRedis(host=redis_host,port=redis_port)

    atexit.register(shutdown_hook,consumer)

    for msg in consumer:
        logger.info('receive data from kafka client')
        redis_client.publish(channel=redis_channel,message=msg.value)
        logger.info('send data %s to redis' % str(msg))

