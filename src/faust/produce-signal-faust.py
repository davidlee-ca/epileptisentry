import boto3
from time import time
import faust


# Get EEG data in csv format from S3. By default, MVP data will be fetched.
def get_eeg_object_from_s3(my_subject_id, mybucket='speegs-source-chbmit'):
    mykey = f"{my_subject_id}/{my_subject_id}_03.csv"
    s3 = boto3.client('s3')
    s3obj = s3.get_object(Bucket=mybucket, Key=mykey)
    return s3obj['Body']


class EEGKey(faust.Record, serializer='json'):
    subject: str
    ch: str

class EEGReading(faust.Record, serializer='json'):
    timestamp: float
    v: float


app = faust.App(
    'test-produce-eeg-signal',
    broker='kafka://ec2-3-217-164-59.compute-1.amazonaws.com:9092'
)

my_topic = app.topic('eeg-signal', key_type=EEGKey, value_type=EEGReading)


@app.timer(interval=0.004)
async def dev_message_producer(app):

    current_time = time()
    readings = str(obj._raw_stream.readline().strip()).split(',')

    for i in range(23):
        if not 5 == i:
            continue
        await my_topic.send(
            key=EEGKey(subject=subject_id, ch=channels[i]),
            value=EEGReading(timestamp=current_time, v=readings[i + 1])
        )

if __name__ == "__main__":

    subject_id = 'chb01'  # for MVP
    channels = (
    'FP1-F7', 'F7-T7', 'T7-P7', 'P7-O1', 'FP1-F3', 'F3-C3', 'C3-P3', 'P3-O1', 'FP2-F4', 'F4-C4', 'C4-P4', 'P4-O2',
    'FP2-F8', 'F8-T8', 'T8-P8', 'P8-O2', 'FZ-CZ', 'CZ-PZ', 'P7-T7', 'T7-FT9', 'FT9-FT10', 'FT10-T8', 'T8-P8')

    obj = get_eeg_object_from_s3(subject_id)
    obj._raw_stream.readline() # throw away the first line
    app.main()
