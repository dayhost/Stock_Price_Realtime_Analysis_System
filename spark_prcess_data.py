from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaConfigurationError, KafkaError
from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra.streaming import joinWithCassandraTable
import logging
import json
import ConfigParser
import datetime
import time
import os

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('spark_stateful_process')
logger.setLevel(logging.INFO)

def read_config():
    parser=None
    config_path = os.path.split(os.path.realpath(__file__))[0]

    try:
        parser = ConfigParser.ConfigParser()
        parser.read(config_path+'/spark_config.ini')
    except Exception:
        logger.error('can\'t find config file')

    spark_dict = {'app_name': parser.get('spark_config','appName'),'master':parser.get('spark_config','master'),
                  'log_level':parser.get('spark_config','log_level'),'time_window':parser.getint('spark_config','time_window')}
    kafka_dict = {'cluster':parser.get('kafka_config','cluster'),'topic_in':parser.get('kafka_config','topic_in'),
                'topic_out':parser.get('kafka_config','topic_out')}
    cassandra_dict = {
        'cluster':parser.get('cassandra_config','cluster'),
        'keyspace':parser.get('cassandra_config','keyspace'),
        'table_user_stock':parser.get('cassandra_config','table_user_stock')
    }

    return spark_dict, kafka_dict, cassandra_dict

def preprocess_data(data):
    """
    :param 'data' structure:
    [
        {
            u'Index': u'OTCMKTS', 
            u'LastTradeWithCurrency': u'0.29', 
            u'LastTradeDateTime': u'2017-05-09T16:00:00Z', 
            u'LastTradePrice': u'0.290', 
            u'LastTradeTime': u'4:00PM EDT', 
            u'LastTradeDateTimeLong': u'May 9, 4:00PM EDT', 
            u'StockSymbol': u'AMAZ', 
            u'ID': u'5156506'
        }
    ]
    :return:
     {
        symbol,
        {
            'trade_time_stamp':(str)time
            'trade_time_long':(long)time  
            'trade_price':(float)price
        }
     }
    """

    parsed = json.loads(data[1].decode('utf-8'))[0]
    symbol = parsed.get('StockSymbol')
    trade_price = float(parsed.get('LastTradePrice'))
    trade_time_stamp = datetime.datetime.strptime(parsed.get('LastTradeDateTime'), '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    trade_time_long = time.mktime(datetime.datetime.strptime(parsed.get('LastTradeDateTime'), '%Y-%m-%dT%H:%M:%SZ').timetuple())

    return symbol,[{'trade_time_stamp':trade_time_stamp, 'trade_time_long':trade_time_long, 'trade_price':trade_price}]


def compute_stock_tending_in_window(dict_list):
    """
    :param value_pair_iter: 
    the structure of this input should be:
     [
        {
            'trade_time_stamp':(str)time
            'trade_time_long':(long)time  
            'trade_price':(float)price
        },
        .......
     ]
    :return:
    the output data should be like this
    {
        'tending':value1',
        'timestamp':time1
    } 
    """

    def compute_tending(dict_list):
        sum = 0.0
        for i in range(0,len(dict_list)-1):
            sum = sum + (dict_list[i+1]['trade_price']-dict_list[i]['trade_price'])

        if(sum<(-0.00005)):
            return -1
        elif(sum>(0.00005)):
            return 1
        else:
            return 0


    # - data here is [dict1,dict2,dict3,.......]
    # - we should sort the list before process it using timestamp
    sorted_stock_list=sorted(dict_list,key=lambda x:x['trade_time_long'],reverse=False)

    # - put the computing result into list

    return {
        'tending':compute_tending(sorted_stock_list),
        'timestamp':sorted_stock_list[len(sorted_stock_list)-1]['trade_time_stamp']
    }


def send_alert_to_kafka(iterator,kafka_config):
    """
    :param iterator: 
    (
        stock_symbol,
        {
            'tending':value1',
            'timestamp':time1
        }
    )
    .......
    :param kafka_config:
    {
        'cluster':parser.get('kafka_config','cluster'),
        'topic_in':parser.get('kafka_config','topic_in'),
        'topic_out':parser.get('kafka_config','topic_out')
    }
    :return: None
    """
    producer = KafkaProducer(bootstrap_servers=kafka_config['cluster'])

    for alert in iterator:
        value = json.dumps({
            'stock_symbol':alert[0],
            'tending':alert[1]['tending']
        })
        producer.send(topic=kafka_config['topic_out'],value=value)

    producer.close()

def aggregate_list(list_a,list_b):
    """
    :param list_a: the parent list
    :param list_b: only contain 1 element in the array
    :return: a list
    """
    list_a.append(list_b[0])
    return list_a


def streaming_logic():
    """
    :function: initial spark context and all the streaming logic
    :return: None
    """

    # - read configuration from file
    spark_config, kafka_config, cassandra_config = read_config()

    # - initial spark context
    conf = SparkConf().setMaster(spark_config['master']).setAppName(spark_config['app_name']).set('spark.cassandra.connection.host', cassandra_config['cluster'])
    csc = CassandraSparkContext(conf=conf)
    csc.setLogLevel(spark_config['log_level'])
    ssc = StreamingContext(sparkContext=csc, batchDuration=spark_config['time_window'])

    # - creating kafka stream
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_config['topic_in']], {'metadata.broker.list': kafka_config['cluster']})

    # - start to process data
    # - output data structure: MetadData
    structured_stock_data = directKafkaStream.map(lambda data : preprocess_data(data=data))
    structured_stock_data.pprint(20)

    stock_data_list = structured_stock_data.reduceByKey(lambda a,b : aggregate_list(a,b))
    stock_data_list.pprint(20)

    # - get history data from cassandra
    alert_user_data = stock_data_list.mapValues(lambda dictlist : compute_stock_tending_in_window(dict_list=dictlist))
    alert_user_data.pprint(20)

    # - send alert to user
    alert_user_data.foreachRDD(lambda rdd : rdd.foreachPartition(lambda iter : send_alert_to_kafka(iterator=iter,kafka_config=kafka_config)))

    ssc.start()
    ssc.awaitTermination()

if __name__=='__main__':
    streaming_logic()