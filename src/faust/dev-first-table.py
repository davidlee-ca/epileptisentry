import faust
from datetime import datetime

app = faust.App(
    'helloworld-by-country',
    broker='kafka://ip-10-0-0-4.ec2.internal:9092'
)

my_topic = app.topic('greetings')

table_counts = app.Table('mycount', default=float, key_type=str, value_type=str).tumbling(5.0, expires=5.0)
table_sum = app.Table('mysum', default=float, key_type=str, value_type=float).tumbling(5.0, expires=5.0)

def get_message_key(blah):
   (k, v) = blah.items()
   return k

@app.agent(my_topic)
async def process_my_stream(messages):
    async for mykey, mymessage in messages.items():
        async for payload in messages.group_by(mykey):
            reading = float(mymessage.split(',')[1])
            table_counts[mykey] += 1
            table_sum[mykey] += reading

@app.timer(2.0)
async def report_every_other_second():
    print(f"{datetime.now()}: we have the following state in table_counts:")
    print(table_counts)
    print("and in table_sum:")
    print(table_sum)


if __name__ == '__main__':
    app.main()
