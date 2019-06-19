import faust

app = faust.App(
    'hello-world',
    broker='kafka://ip-10-0-0-4.ec2.internal:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('greetings')

@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        print(greeting)