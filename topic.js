const { Kafka } = require('kafkajs')
require('dotenv').config();
run()
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "testKafka",
            "brokers": [`${process.env.KAFKA_BROKER_IP}:9092`],
        })
        const admin = kafka.admin()
        console.log("Connecting...")
        await admin.connect()
        console.log("Connected...")
        // A-M, N-Z
        await admin.createTopics({
            "topics": [{
                "topic": "Users",
                "numPartitions": "2"
            }]
        })
        console.log("Created Successfully")
        await admin.disconnect()
    }
    catch (ex) {
        console.error(`Something bad happened ${ex}`)
    }
    finally {
        process.exit(0)
    }
}