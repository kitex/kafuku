from flask import Flask,render_template,jsonify,request
import time
from datetime import datetime
import os
from confluent_kafka import OFFSET_BEGINNING,Producer,Consumer,KafkaError
from kmessages import Kmessage
from confluent_kafka.admin import AdminClient

app = Flask(__name__)

# Function to load config from .properties file
def load_kafka_config(filename="kafka.properties"):
    config = {}
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):  # Ignore comments and empty lines
                key, value = line.split("=", 1)
                print(f"Loading config: {key.strip()} = {value.strip()}")
                config[key.strip()] = value.strip()
    return config

def load_application_config(filename="config.properties"):
    config = {}
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):  # Ignore comments and empty lines
                key, value = line.split("=", 1)
                print(f"Loading config: {key.strip()} = {value.strip()}")
                config[key.strip()] = value.strip()
    return config

# Initial load of config
kafka_config = load_kafka_config()
app_config = load_application_config()
app.config.update(kafka_config)
app.config.update(app_config)
# Track the last modification time of the config file
last_modified_time = os.path.getmtime("kafka.properties")
kadmin = AdminClient(kafka_config)
# Fetch metadata to get topics
metadata = kadmin.list_topics(timeout=10)


def delivery_report(err, msg):
    """ Callback function to check if the message was successfully delivered """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

@app.route("/")
def home():    
   
    return render_template("table.html",ktopics_json=ktopics_json)


@app.route("/listtopics")
def home():    
    ktopics = list(metadata.topics.keys())
    ktopics_json = [{"topic": topic} for topic in ktopics]
    return jsonify(ktopics_json)

@app.route("/loadmessages", methods=["GET"])
def load_messages():
    print("Loading messages...")	
    messages = consume_kafka("run")
    data = [{"id": msg.id, "message_json": msg.message_json,"timestamp_type":msg.timestamp_type,"timestamp_value":msg.timestamp_value} for msg in messages]
    print(request.args)
    return jsonify(data)

@app.route("/pushmessage",methods=['POST'])
def push_message():
    producer = Producer(kafka_config)
    try:
        # Get JSON data from request
        data = request.get_json()

        if not data:
            return jsonify({"status": "error", "message": "No data received"}), 400

        # Extract parameters from JSON
        selected_messages = data.get("messages", [])
        extra_param = data.get("extra_param", "default_value")

        print(f"📩 Received Messages: {selected_messages}")
        print(f"🔹 Extra Param: {extra_param}")

        kafka_topic = app_config.get('kafka_topic')  # Replace with actual Kafka topic

        # Publish each message to Kafka
        for msg in selected_messages:
            message_str = str(msg)  # Convert dict to string
            producer.produce(kafka_topic, key=str(msg["id"]), value=message_str, callback=delivery_report)
        
        producer.flush()  # Ensure all messages are sent
        status = "success"
        code = 200

    except Exception as e:
        status = "error"
        selected_messages =str(e)
        code = 500
    finally:
        return jsonify({"status": status, "received_messages": selected_messages}),code


def consume_kafka(fromwhen):
    consumer = Consumer(kafka_config)
    if fromwhen != 'run':
        return "not executing"
   
    # Subscribe to the desired topic(s)
    kafka_topic = app_config.get('kafka_topic')
    consumer.subscribe([kafka_topic])
    
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
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                print("Error: ", msg.error())
                continue
            # Get message timestamp
            timestamp_type, timestamp_value = msg.timestamp()
            readable_time = datetime.fromtimestamp(timestamp_value / 1000).strftime('%Y-%m-%d %H:%M:%S')

            print("Received message: ", msg.value().decode('utf-8'))
            kafka_message = Kmessage(False,msg.value().decode('utf-8'),timestamp_type,readable_time)
            messages.append(kafka_message)
    except KeyboardInterrupt:
        # Exit gracefully on Ctrl+C
        pass
    finally:
        # Close down consumer to commit final offsets.
        print('closing connection')
        consumer.close()

    return messages

''''
def read_kafka():
    kafka_topic = app_config.get('kafka_topic')
    consumer.subscribe([kafka_topic])

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
        '''