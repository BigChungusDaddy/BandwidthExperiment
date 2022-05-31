import sys
import pika
import time

class Sender:
    def __init__(self):
        self.messageSize = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
        self.messages = self.createMessage(self.messageSize)
        # Interval of how much the sender needs to wait after finish one round of sending.
        self.waitTime = 5
        self.secToRun = 5
        self.numOfRepeat = 5
        self.messageAmount = 0
        # Need to change the connection parameter
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange = 'bandwidthExperiment', exchange_type='fanout')

    
    # Create messages and store them in a queue.
    def createMessage(self, messageSize):
        messages = []
        for eachSize in messageSize:
            messages.append('a' * eachSize)
        return messages

    def sendMessages(self):
        print ("[x] Sending start.")
        for currentIndex in range(7):
            currentMessage = self.messages[currentIndex]
            currentMessageSize = self.messageSize[currentIndex]
            for x in range(self.numOfRepeat):
                endTime = time.time() + self.secToRun
                while time.time() < endTime:
                    # Note: The rabbitmq server is responsible for flow control.
                    self.channel.basic_publish(exchange='bandwidthExperiment', routing_key = '', body = currentMessage)
                    self.messageAmount += 1
                print("[x] Sent %s messages in %s seconds, each message is %s bytes" % (
                    self.messageAmount, 
                    self.secToRun, 
                    currentMessageSize))
                self.messageAmount = 0
                time.sleep(self.waitTime)
        print ("[x] Sending ends.")
        self.connection.close()

sender = Sender()
sender.sendMessages()