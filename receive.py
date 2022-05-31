import pika
import sys
import threading

# Since we are testing the throughput, the consumer should not be a limiting factor, hence it will consume as fast as possible
accumulativeAmount = 0

credential = pika.PlainCredentials('receiver', 'thisisreceiver')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='192.168.1.1', credentials=credential))
channel = connection.channel()

channel.exchange_declare(exchange='bandwidthExperiment', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='bandwidthExperiment', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    accumulativeAmount += 1

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()

def printAccAmount():
  threading.Timer(5.0, printAccAmount).start()
  print(str(accumulativeAmount))