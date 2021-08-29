const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const producer = kafka.producer()

async function produce() {
    try {
        await producer.connect()
        await producer.send({
            topic: 'test-topic',
            messages: [{
                key: 'key1',
                value: 'hello world',
                headers: {
                    'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
                    'system-id': 'my-system'
                }
            }],
        })

        await producer.disconnect()
    } catch (err) {
        console.error(err);
    }
}

produce()
    .then(() => console.log('Produce'))
    .catch(console.error)
