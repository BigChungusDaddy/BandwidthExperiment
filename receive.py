import pika
import csv
import time

# Since we are testing the throughput, the consumer should not be a limiting factor, hence it will consume as fast as possible

receivedAmount = 0
# startLogging = False
# currentMessageSize = 256
timerStarted = False
timerInterval = 1
previousTime = 0
pastTime = 1

with open('data.csv','w', newline='') as datacsv:
    header = ['Past Time', 'Total Message Amount']
    writer = csv.writer(datacsv)
    writer.writerow(header)

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
    #deliveryTag = method.delivery_tag
    #ch.basic_ack(delivery_tag = deliveryTag, multiple = True)

    global receivedAmount
    # global startLogging
    # global currentMessageSize
    global timerStarted
    global previousTime
    global timerInterval
    global pastTime

    # Check for new second
    if not timerStarted:
        timerStarted = True
        previousTime = time.time()
    if timerStarted and time.time() < previousTime + timerInterval:
        receivedAmount += 1
    if timerStarted and time.time() >= previousTime + timerInterval:
        previousTime = time.time()
        logData(pastTime, receivedAmount)
        receivedAmount = 0
        pastTime += 1


    # # The beginning of one sending burst
    # if len(body) == 3 or len(body) == 4 or len(body) == 5 or len(body) == 6 or len(body) == 7:
    #     startLogging = True
    #     currentMessageSize = int(body.decode("utf-8"))
    # # The end of one sending burst
    # if len(body) == 1:
    #     startLogging = False
    #     # Minus one since the very first overhead is counted
    #     logData(currentMessageSize, receivedAmount - 1)
    #     receivedAmount = 0
    # if startLogging:
    #     receivedAmount += 1


def logData(pastTime, messageAmount):
    with open('data.csv','a', newline='') as datacsv:
        writer = csv.writer(datacsv)
        row = [str(pastTime), str(messageAmount)]
        writer.writerow(row)
    print("[x] Received %s messages in the past 1 second. Elapsed %s seconds." % (
        messageAmount, 
        pastTime))
    print("[x] There are %s messages remaining in the queue." % (result.method.message_count))

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack= True)

channel.start_consuming()
