import pika
import sys

# Since we are testing the throughput, the consumer should not be a limiting factor, hence it will consume as fast as possible

credential = pika.PlainCredentials('receiver', 'thisisreceiver')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(pika.ConnectionParameters(host='192.168.1.1',credentials=credential)))
channel = connection.channel()

channel.exchange_declare(exchange='bandwidthExperiment', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='bandwidthExperiment', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    pass

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()