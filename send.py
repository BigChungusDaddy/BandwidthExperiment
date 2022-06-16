import csv
import pika
import time

class Sender:
    def __init__(self):
        # self.messageSize = [256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152]
        self.messageSize = [4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288]
        self.messages = self.createMessage(self.messageSize)
        # Interval of how much the sender needs to wait after finish one round of sending.
        self.waitTime = 0.1
        self.secToRun = 1
        self.numOfRepeat = 1000
        self.messageAmount = 0
        self.sendStarted = False
        self.credential = pika.PlainCredentials('sender', 'thisissender')
        # Need to change the connection parameter
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='192.168.1.2', credentials= self.credential))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange = 'bandwidthExperiment', exchange_type='fanout')
        with open('data.csv','w', newline='') as datacsv:
            header = ['Message Size in Bytes', 'Total Message Amount']
            writer = csv.writer(datacsv)
            writer.writerow(header)

    
    # Create messages and store them in a queue.
    def createMessage(self, messageSize):
        messages = []
        for eachSize in messageSize:
            messages.append('a' * eachSize)
        return messages

    def sendMessages(self):
        print ("[x] Sending start.")
        for currentIndex in range(len(self.messageSize)):
            currentMessage = self.messages[currentIndex]
            currentMessageSize = self.messageSize[currentIndex]
            for x in range(self.numOfRepeat):
                endTime = time.time() + self.secToRun
                while time.time() < endTime:
                    if not self.sendStarted:
                        # Starting Message
                        self.channel.basic_publish(exchange='bandwidthExperiment', routing_key = '', body = str(currentMessageSize))
                        self.sendStarted = True
                    # Note: The rabbitmq server is responsible for flow control.
                    self.channel.basic_publish(exchange='bandwidthExperiment', routing_key = '', body = currentMessage)
                    self.messageAmount += 1
                print("[x] Sent %s messages in %s seconds, each message is %s bytes" % (
                    self.messageAmount, 
                    self.secToRun, 
                    currentMessageSize))
                
                self.channel.basic_publish(exchange='bandwidthExperiment', routing_key = '', body = 'x')
                self.sendStarted = False

                self.logData(currentMessageSize, self.messageAmount)
                self.messageAmount = 0
                print("[x] Now sleeping for 0.1 seconds.")
                time.sleep(self.waitTime)
        print ("[x] Sending ends.")
        self.connection.close()
    
    def logData(self, messageSize, messageAmount):
        with open('data.csv','a', newline='') as datacsv:
            writer = csv.writer(datacsv)
            row = [str(messageSize), str(messageAmount)]
            writer.writerow(row)

sender = Sender()
sender.sendMessages()