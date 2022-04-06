const express = require('express');
const path = require('path');
const app = express();
const http = require('http');
const server = http.createServer(app);
const { Server } = require("socket.io");
const io = new Server(server);
const { Kafka } = require('kafkajs')

// Create the client with the broker list
const kafka = new Kafka({
  brokers: ['redpanda-0.larsenpanda.com:9093'],
  clientId: 'socketio-fun',
})

const topic = 'hometemp';
const cg = 'test-group';

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: cg })

//Instantiate the consumer function
const run = async () => {

    //Connect to Redpanda
    await consumer.connect()

    //Create the subscription to the topic in Redpanda
    await consumer.subscribe({ topic })

    //Start polling on the Redpanda consumer
    await consumer.run({
      //Each Message pass the topic, partition, and message itself:
      eachMessage: async ({ topic, partition, message }) => {
        const prefix = `${topic}[${partition} | ${message.offset}]`
        console.log(`- ${prefix} ${message.key}#${message.value} - ${message.timestamp}`)

        //Send a message to the socket.io channel with the same name as the Redpanda topic
        io.emit(topic,`${message.value}`)
      },
    })
}

//Run the consumer function
run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

io.on('connection', (socket) => {

    //When we get a message from the web page (input sends to "chat message") produce it to redpanda's "hometemp" topic
    socket.on('web input', (msg) => {
      const run = async () => {
        await producer.connect()
        console.log('message: ' + msg);
        await producer.send({
          topic: topic,
          messages: [
            { value: msg },
          ],
        })
        await producer.disconnect()
      }
      run();
    })

});

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, '/index.html'));
});

server.listen(3000, () => {
  console.log('listening on *:3000');
});
