# from confluent_kafka import Producer
# from confluent_kafka.admin import AdminClient, NewTopic
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import boto3
import pandas as pd


# DEV
subject_id = 'chb01'
"""
kafka_producer_conf = {
    'bootstrap.servers': [
        'ip-10-0-0-8.ec2.internal:9092',
        'ip-10-0-0-11.ec2.internal:9092',
        'ip-10-0-0-4.ec2.internal:9092'],
    'group.id': f"eeg_{subject_id}"
}
kafka_admin_conf = {
    'bootstrap.servers': [
        'ip-10-0-0-8.ec2.internal:9092',
        'ip-10-0-0-11.ec2.internal:9092',
        'ip-10-0-0-4.ec2.internal:9092'],
    'debug': 'broker,admin'
    }
"""


# Get EEG data in csv format from S3. By default, MVP data will be fetched.
def get_eeg_object_from_s3(mybucket='speegs-source-chbmit', mykey=f"{subject_id}/{subject_id}_03.csv"):
    s3 = boto3.client('s3')
    s3obj = s3.get_object(Bucket=mybucket, Key=mykey)
    return s3obj


# For development only
def delete_topics(a, topics):
    """ delete topics """
    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
#    fs = a.delete_topics(topics, operation_timeout=30)
    fs = a.delete_topics(topics, timeout_ms=20000)
"""
    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} deleted")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")
"""


def initialize_topics(a, patients, retry_on_error=True):
    patienttopics = list(map(lambda p: NewTopic(patients, 3, 3), patients))
    fs = a.create_topics(patienttopics)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print (f"Failed to create topic {patients}: {e}")
            if retry_on_error:
                print ("Will attempt to delete the topic and create it again...")
                delete_topics(a, list(topic))


def produce_eeg_messages(p, patient):

    obj = get_eeg_object_from_s3()
    df = pd.read_csv(obj['Body'], header=0)
    keyLabels = list(df)

    for row in df:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        for channel in range(1,24):
            p.produce(patient, key=keyLabels[channel], value=f"{row[0]}:{row[channel]}", callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":

    #    p = Producer(kafka_producer_conf)
    #    a = AdminClient(kafka_admin_conf)
    p = KafkaProducer(bootstrap_servers='ip-10-0-0-8.ec2.internal:9092')
    a = KafkaAdminClient(bootstrap_servers='ip-10-0-0-8.ec2.internal:9092')


    initialize_topics(a, subject_id)
    produce_eeg_messages(p, subject_id)

