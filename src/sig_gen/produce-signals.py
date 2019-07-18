"""
This script reads preprocessed electroencephalography (EEG) data from the CHB-MIT Scalp EEG Database [1] from a S3 bucket
and produces them as Kafka messages to emulate an EEG instrument interfaced with the epileptiSentry system.

Author:
David Lee, Insight Data Engineering Fellow, New York City 2019

Reference:
[1] https://physionet.org/pn6/chbmit/
"""


from confluent_kafka import Producer
from time import time, sleep
import boto3
import sys


topic = "eeg-signal"
brokers = "10.0.1.36:9092,10.0.1.27:9092,10.0.1.11:9092,10.0.1.54:9092,10.0.1.61:9092"


if __name__ == '__main__':

    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <subject_id>\n' % sys.argv[0])
        sys.exit(1)

    subject_id = sys.argv[1]

    frequency = 256.0
    delay = 0.80 / frequency
    refresh_delay_interval = 5.0

    # Instantiate a Kafka Producer
    # For producer configuration, see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': brokers,
            'group.id': 'eeg-player'}
    p = Producer(**conf)

    # Grab the list of EEG files from the S3 bucket, and replay them sequentially
    my_bucket = 'speegs-source-chbmit'
    s3 = boto3.client('s3')
    my_objects = s3.list_objects_v2(Bucket=my_bucket, Prefix=subject_id)['Contents']
    list_of_s3_keys = sorted([ x['Key'] for x in my_objects ])

    for mykey in list_of_s3_keys:

        obj = s3.get_object(Bucket=my_bucket, Key=mykey)['Body']
        line = obj._raw_stream.readline() # throw away the first line

        channels = (
            'FP1-F7', 'F7-T7', 'T7-P7', 'P7-O1', 'FP1-F3', 'F3-C3', 'C3-P3', 'P3-O1', 'FP2-F4', 'F4-C4', 'C4-P4', 'P4-O2',
            'FP2-F8', 'F8-T8', 'T8-P8', 'P8-O2', 'FZ-CZ', 'CZ-PZ', 'P7-T7', 'T7-FT9', 'FT9-FT10', 'FT10-T8', 'T8-P8.1')

        count = 0
        start_time = time()
        heartbeat = time()

        for line in obj._raw_stream:
            readings = str(line.decode().strip()).split(',')

            for i in range(23):
                key = '{"subject": "%s", "ch": "%s"}' % (subject_id, channels[i])
                value = '{"timestamp": %.6f, "v": %.6f}' % (start_time + float(readings[0]), float(readings[i + 1]))
                p.produce(topic, value=value, key=key)

            sleep(delay)
            count += 1

            # For efficiency, batch 8 frames (~1/32 sec) before flushing the Kafka producer queue
            if count % 8 == 0:  
                p.flush()

            # Adjust the sleeping interval every refresh_delay_interval seconds
            if count == (refresh_delay_interval * frequency):  

                new_heartbeat = time()
                duration = new_heartbeat - heartbeat
                deviation = (refresh_delay_interval - duration) * 1000

                try:
                    delay = delay + deviation / (refresh_delay_interval * 1000) / frequency * 0.5  # 0.5 = dampening factor
                    if delay < 0:
                        raise ValueError
                except ValueError:
                    delay = 0
                    print("WARNING: NEW DELAY TIME INTERVAL WAS A NEGATIVE NUMBER. Setting to 0..")

                print(f"5-second check in for {subject_id}. Deviation: {deviation:.2f} ms, new delay: {delay * 1000:.2f} ms.")
                count = 0
                heartbeat = new_heartbeat

        p.flush()  # flush the remaining signal before termination
