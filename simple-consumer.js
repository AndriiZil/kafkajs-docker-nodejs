const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'test-group' })

async function consume() {
    try {
        await consumer.connect()
        await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                    key: message.key.toString(),
                    value: message.value.toString(),
                    headers: {
                        'correlation-id': message.headers['correlation-id'].toString(),
                        'system-id': message.headers['system-id'].toString(),
                    },
                })
            },
        })
    } catch (err) {
        console.error(err)
    }
}

consume()
    .then(() => console.log('Consume'))
    .catch(console.error)
