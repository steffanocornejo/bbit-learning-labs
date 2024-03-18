from producer_interface import *
import pika 
import os

class mqProducer(mqProducerInterface):
    
    def __init__(self, routing_key, exchange_name):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

        # self.chnl = None
        # self.cntc = None

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        self.cntc = connection
        # Establish Channel
        channel = connection.channel()
        self.chnl = channel
        # Create the exchange if not already present
        channel.exchange_declare(exchange=self.exchange_name)

    def publishOrder(self, message: str):
        self.chnl.basic_publish(exchange=self.exchange_name,
        routing_key="Routing Key",
        body="Message")
        # Basic Publish to Exchange

    def __del__(self):
        self.chnl.close()
        # self.cntc.close()


    

    
