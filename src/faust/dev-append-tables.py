import faust
from typing import List
from time import time


class MyTopicKey(faust.Record, serializer='json'):
    foo: str


# class MyTopicValue(faust.Record, isodates=True, serializer='json'):
class MyTopicValue(faust.Record, serializer='json'):
    timestamp: float
    bar: float


class LOL(faust.Record):
    container: List[MyTopicValue]


dev_app = faust.App('dev-app-1', broker='kafka://ec2-52-23-190-192.compute-1.amazonaws.com:9092', topic_partitions=1)

dev_topic = app.topic('dev-channel-1', key_type=MyTopicKey, value_type=MyTopicValue)

dev_table = app.Table('dev-table-1', default=LOL)


@dev_app.agent(dev_topic)
async def repartition_by_key(messages):
    async for message in messages.group_by(MyTopicKey.foo):
        pass


@dev_app.timer(interval=0.45)
async def dev_message_producer(dev_app):
    await repartition_by_key.send(
        key=MyTopicKey(foo="foo"),
        value=MyTopicValue(timestamp=time(), bar=0.123),
    )


if __name__ == '__main__':
    dev_app.main(



class MyTopicKey(faust.Record, serializer='json'):
    userid: str

class MyTopicValue(faust.Record, serializer='json'):
    timestamp: float
    value: float

app = faust.App('app1', broker='localhost:9092')
data_topic = app.topic('data', key_type=MyTopicKey, value_type=MyTopicValue)

@dev_app.agent()
async def periodically_analyze_all_values(stream):
    async for values in stream.take(1024, within=10.0):
        metric = complicated_method(values)
        yield metric

@dev_app.agent(dev_topic)
async def repartition_by_key(messages):
    async for key, my_message in messages.group_by(MyTopicKey.userid).items():
        metric = await periodically_analyze_all_values(my_message.value)
        print(f"The rolling metric for {key.userid} is {metric}.")