import urllib
import pika
import json
import ssl

class RabbitMq:
    endpoint = ""
    ssl_options = {}
    connection_url = ""
    connection = None
    channel = None
    logger = None

    def __init__(self, options, logger):
        self.endpoint = options["AMQP"]
        self.logger = logger
        rabbit_options = {'heartbeat': 20}
        if len(options) >= 4 and "RABBITCA" in options and "RABBITCERT" in options and "RABBITKEY" in options:
            self.ssl_options = {'ca_certs': options["RABBITCA"], 'certfile': options["RABBITCERT"],
                                'keyfile': options["RABBITKEY"] , }
            rabbit_options["ssl_options"] = self.ssl_options
        self.connection_url = self.endpoint + "?" + urllib.parse.urlencode(rabbit_options)

        self.connection = pika.BlockingConnection(pika.connection.URLParameters(self.connection_url))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)

    def consume(self, queue, callback):
        print(queue)
        self.channel.queue_declare(queue=queue)
        self.channel.basic_consume(queue,callback,auto_ack=True)
	#To be used if pika==0.10.0        
	#self.channel.basic_consume(callback, queue=queue, no_ack=True)
        self.channel.start_consuming()

    def send_message(self, queue, message, correlation_id=None):
        rabbitData = str(json.dumps({'res': message}))
        properties = pika.spec.BasicProperties(correlation_id=correlation_id) if correlation_id else None
        self.channel.basic_publish(exchange='', routing_key=queue, body=rabbitData, properties=properties)
