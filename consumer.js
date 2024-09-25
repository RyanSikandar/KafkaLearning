const { Kafka } = require('kafkajs')
require('dotenv').config();


run()
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "testKafka",
            "brokers": [`${process.env.KAFKA_BROKER_IP}:9092`],
        })
        const consumer = kafka.consumer({ "groupId": "test" })
        console.log("Connecting...")
        await consumer.connect()
        console.log("Connected...")

        await consumer.subscribe({
            "topic": "Users",
            "fromBeginning": true
        })

        await consumer.run({
            "eachMessage": async result => {
                console.log(`Received message => ${result.message.value.toString()} on partition ${result.partition}`)
            }
        })
    }
    catch (ex) {
        console.error(`Something bad happened ${ex}`)
    }
    finally {
    }
}