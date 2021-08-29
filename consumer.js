const {Kafka} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({groupId: 'test-group'})

async function consume() {
    try {
        await consumer.connect()
        await consumer.subscribe({topic: 'payment', fromBeginning: true})
        await consumer.subscribe({topic: 'email', fromBeginning: true})

        await consumer.run({
            eachBatchAutoResolve: true,
            eachBatch: async ({
                                  batch,
                                  resolveOffset,
                                  heartbeat,
                                  commitOffsetsIfNecessary,
                                  uncommittedOffsets,
                                  isRunning,
                                  isStale,
                              }) => {
                for (let message of batch.messages) {
                    console.log({
                        topic: batch.topic,
                        partition: batch.partition,
                        highWatermark: batch.highWatermark,
                        message: {
                            offset: message.offset,
                            key: message.key.toString(),
                            value: message.value.toString(),
                            headers: message.headers.toString(),
                        }
                    })

                    resolveOffset(message.offset)
                    await heartbeat()
                }
            },
        })
    } catch (err) {
        console.error(err);
    }
}

consume()
    .then(() => console.log('Consume'))
    .catch(console.error);
