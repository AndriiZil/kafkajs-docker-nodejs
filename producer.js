const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const topicMessages = [
    {
        topic: 'email',
        messages: [
            {
                key: 'email',
                value: 'hello email',
                headers: {
                    'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
                },
            }
        ],

    },
    {
        topic: 'payment',
        messages: [
            {
                key: 'payment',
                value: 'hello payment',
                headers: {
                    'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
                },
            }
        ],

    }
]

const producer = kafka.producer()

async function produce() {
    try {
        await producer.connect()
        await producer.sendBatch({ topicMessages })
        await producer.disconnect();
    } catch (err) {
        console.error(err);
    }
}

produce()
    .then(() => console.log('DONE'))
    .catch(console.error);
