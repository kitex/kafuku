from flask import Flask,render_template
from confluent_kafka import Consumer, KafkaError
import time
from confluent_kafka import OFFSET_BEGINNING
from kmessages import Kmessage

app = Flask(__name__)

@app.route("/")
def hello_world():
    messages = consume_kafka("run")
    return render_template("table.html",kafka_messages=messages)


def kafka_config():
     # Consumer configuration settings
    conf = {
        'bootstrap.servers': '192.168.0.23:9092',  # Replace with your broker address(es)
        'group.id': 'kafka-reader-00003-rock',          # Unique group id for this consumer group
        'auto.offset.reset': 'earliest',           # Start reading at the beginning if no offset is stored
        'enable.auto.commit': False 
    }

    # Create the Consumer instance
    consumer = Consumer(conf)
    return  consumer

def consume_kafka(fromwhen):
    if fromwhen != 'run':
        return "not executing"
   
    consumer = kafka_config()
    # Subscribe to the desired topic(s)
    topic = '1mcs.document.deliver'  # Replace with your topic name
    consumer.subscribe([topic])
    
    messages = []

    while not consumer.assignment():
        consumer.poll(1.0)

    
    # Retrieve the current partition assignments
    partitions = consumer.assignment()

    # Set each partition’s offset to the beginning
    for tp in partitions:
        tp.offset = OFFSET_BEGINNING

    # Re-assign the partitions with the new offsets
    consumer.assign(partitions)

    # Now the consumer will start from the beginning for each partition
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break
        if msg.error():
            print("Error: ", msg.error())
            continue
        print("Received message: ", msg.value().decode('utf-8'))
        kafka_message = Kmessage(False,msg.value().decode('utf-8'))
        messages.append(kafka_message)

    return messages


def read_kafka(): 
    consumer = kafka_config()

    # Subscribe to the desired topic(s)
    topic = '1mcs.document.deliver'  # Replace with your topic name
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages (timeout in seconds)
            print('polling')
            msg = consumer.poll(1.0)

            if msg is None:
                # No message available within the timeout period
                continue

            if msg.error():
                # Handle any error from Kafka
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'Reached end of partition: {msg.topic()}/{msg.partition()}')
                else:
                    # Some other error
                    print(f'Error: {msg.error()}')
            else:
                # Successfully received a message
                message_value = msg.value().decode('utf-8')
                print(f'Received message: {message_value}')
    except KeyboardInterrupt:
        # Exit gracefully on Ctrl+C
        pass
    finally:
        # Close down consumer to commit final offsets.
        print('closing connection')
        consumer.close()