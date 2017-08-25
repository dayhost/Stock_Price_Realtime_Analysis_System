from flask import Flask
from flask import request
from flask import jsonify
from kafka import KafkaProducer
from googlefinance import getQuotes
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import ConfigParser
import os
import json
import atexit


app = Flask(__name__)

stock_names_set = set()

@app.route('/add_stock_symbol',methods=['POST'])
def add_stock_stock():
    if request.method == 'POST':
        stock_name = request.form.get('stock_symbol')
        if(stock_names_set.__contains__(stock_name)):
            return jsonify(
                message="stock already exist"
            )
        stock_names_set.add(stock_name)
        return jsonify(
            message='success'
        )

def send_data_to_kafka(producer,topic):
    for stock_symbol in stock_names_set:
        msg = getQuotes(stock_symbol)
        producer.send(topic=topic,value=json.dumps(msg))
        print('the stock symbol is %s and the result is %s' % (str(stock_symbol),str(msg)))

def shutdown_hook(producer,schedulers):
    producer.flush(10)
    producer.close()
    schedulers.shutdown()


if __name__=='__main__':
    parser = ConfigParser.ConfigParser()
    parser.read(os.getcwd() + '/redis_config.ini')
    kafka_cluster = parser.get('kafka_config','cluster')
    kafka_topic = parser.get('kafka_config','topic')

    producer = KafkaProducer(bootstrap_servers=kafka_cluster)

    schedulers = BackgroundScheduler()
    schedulers.add_executor(ThreadPoolExecutor(5))
    schedulers.add_job(send_data_to_kafka, 'interval', [producer, kafka_topic], seconds=3)
    schedulers.start()

    atexit.register(shutdown_hook, producer,schedulers)
    app.run(host='localhost',port=8081)
    # init and run scheduler


