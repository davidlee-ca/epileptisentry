import faust
from datetime import datetime

app = faust.App(
    'helloworld-by-country',
    broker='kafka://ip-10-0-0-4.ec2.internal:9092'
)


class EEGReading(faust.Record, serializer='json'):
    time: float
    potential: float
    channel: str


my_topic = app.topic('greetings', key_type=str, value_type=EEGReading)

order_count_by_account = app.Table('order_count', default=int)

table_counts = app.Table('mycount', default=int, key_type=str, value_type=int).tumbling(5.0, expires=5.0)
table_sum = app.Table('mysum', default=float, key_type=str, value_type=float).tumbling(5.0, expires=5.0)

@app.agent(my_topic)
async def count_readings(readings: faust.Stream[EEGReading]) -> None:
    async for reading in readings.group_by(EEGReading.channel):
        print(f"Received message with timestamp: {reading.time}")
        table_counts[reading.channel] += 1
        table_sum[reading.channel] += reading.potential

@app.timer(2.0)
async def report_every_other_second():
    print(f"   *** {datetime.now()}: we have the following state in table_counts:")
    print(table_counts)
    print("   *** and in table_sum:")
    print(table_sum)


if __name__ == '__main__':
    app.main()
