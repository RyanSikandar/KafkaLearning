const { Kafka } = require('kafkajs')
require('dotenv').config();

const msg = process.argv[2]
run()
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "testKafka",
            "brokers": [`${process.env.KAFKA_BROKER_IP}:9092`],
        })
        const producer = kafka.producer()
        console.log("Connecting...")
        await producer.connect()
        console.log("Connected...")
        // A-M, N-Z
        const partition = msg[0] < "N" ? 0 : 1;
        const result = await producer.send({
            "topic": "Users",
            "messages": [
                {
                    "value": msg,
                    "partition": partition
                }
            ]
        })
        console.log(`Message sent successfully ${JSON.stringify(result)}`)
        await producer.disconnect()
    }
    catch (ex) {
        console.error(`Something bad happened ${ex}`)
    }
    finally {
        process.exit(0)
    }
}