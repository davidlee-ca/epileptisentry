from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import boto3
import pandas as pd


# Get EEG data in csv format from S3. By default, MVP data will be fetched.
def get_eeg_object_from_s3(mybucket='speegs-source-chbmit', mykey=f"{subject_id}/{subject_id}_03.csv"):
    s3 = boto3.client('s3')
    s3obj = s3.get_object(Bucket=mybucket, Key=mykey)
    return s3obj


# Start a new topic with the subject id
def initialize_topics(a, my_subject_id):
    new_patient_topicname = [my_subject_id]
    try:
        a.delete_topics(new_patient_topicname, timeout_ms=30000)
    except Exception as e:
        pass

    new_patient_topic = [NewTopic(my_subject_id,3,3)]
    try:
        a.create_topics(new_patient_topic)
    except Exception as e:
        print (f"Failed to create topic {my_subject_id}: {e}.")


# Push EEG signal from S3 to Kafka
def produce_eeg_messages(p, patient):

    obj = get_eeg_object_from_s3()
    df = pd.read_csv(obj['Body'], header=0)
    keyLabels = list(df)

    for row in df.itertuples(index=False):
        for channel in range(1,24):
            mykey = keyLabels[channel]
            myvalue = f"{row[0]}:{row[channel]}"
            p.send(patient, key=mykey, value=myvalue)

    p.flush()


if __name__ == "__main__":

    subject_id = 'chb01'  # for MVP

    p = KafkaProducer(
        bootstrap_servers='ip-10-0-0-8.ec2.internal:9092',
        key_serializer=lambda m:str.encode(m),
        value_serializer=lambda m:str.encode(m)
    ) # value should be serialized into two floats
    a = KafkaAdminClient(
        bootstrap_servers='ip-10-0-0-8.ec2.internal:9092'
    )

    initialize_topics(a, subject_id)
    produce_eeg_messages(p, subject_id)


"""
# DEV -- was going to use confluent_kafka but realized I have 'vanilla' Kafka installed.
# Will change to confluent_kafka to take advantage of rich callbacks and avro serialization.

    #    p = Producer(kafka_producer_conf)
    #    a = AdminClient(kafka_admin_conf)

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

def delete_topics(a, topics):
    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
#    fs = a.delete_topics(topics, operation_timeout=30)
    a.delete_topics(topics, timeout_ms=30000)
    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} deleted")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")

    patienttopics = list(map(lambda p: NewTopic(patients, 3, 3), patients))
    fs = a.create_topics(patienttopics)
    fs = a.create_topics(list(NewTopic(patients,3,3)))

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print (f"Failed to create topic {patients}: {e}")
            if retry_on_error:
                print ("Will attempt to delete the topic and create it again...")
                delete_topics(a, )

def delivery_report(err, msg):
    # Called once for each message produced to indicate delivery result.
    # Triggered by poll() or flush().
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# from confluent_kafka import Producer
# from confluent_kafka.admin import AdminClient, NewTopic
"""




