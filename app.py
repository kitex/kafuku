from flask import Flask,render_template
from confluent_kafka import Consumer, KafkaError

app = Flask(__name__)

@app.route("/")
def hello_world():
    print("this is test")
    #read_kafka()
    return render_template("index.html")


def read_kafka(): 
    # Consumer configuration settings
    conf = {
        'bootstrap.servers': '192.168.0.23:9092',  # Replace with your broker address(es)
        'group.id': 'my-consumer-group',          # Unique group id for this consumer group
        'auto.offset.reset': 'earliest'           # Start reading at the beginning if no offset is stored
    }

    # Create the Consumer instance
    consumer = Consumer(conf)

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