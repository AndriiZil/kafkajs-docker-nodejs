const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const topics = [
    { topic: 'orderCreated', numPartitions: 2, replicationFactor: 1 },
    { topic: 'payment', numPartitions: 2, replicationFactor: 1 },
    { topic: 'email', numPartitions: 2, replicationFactor: 1 },
    { topic: 'test-topic', numPartitions: 2, replicationFactor: 1 },
]

const process  = async () => {
    const admin = kafka.admin();

    await admin.connect();
    await admin.createTopics({ topics });
    await admin.disconnect();
};

process().then(() => console.log('done'));
