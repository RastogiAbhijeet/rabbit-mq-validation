import express, { Request, Response } from 'express';
import {faker } from "@faker-js/faker"
import { v4 as uuidv4 } from 'uuid';
import amqp from "amqplib"

// import expressStatusMonitor frosm 'express-status-monitor';
// const statusMonitor = expressStatusMonitor({ path : ""})

const host = process.env.HOST ?? 'localhost';
const port = process.env.PORT ? Number(process.env.PORT) : 3000;

const app = express();

const rabbitMQConnection = amqp.connect("amqp://localhost")

function buildMockData ({size = 1} : {size?: number} ) {
  console.log({size})
  return Array(size).fill(null).map((_, index) => ({
    time: Date.now(),
    id: index,
    data: Array(2).fill(null).map((_ ,i) => {
      return {
        id : `${index} | ${i}` , 
        name : faker.person.firstName().slice(0, 10)
      }
    })
  }))
}


app.get('/', (req, res) => {
  res.send({ message: 'Hello API' });
});

app.get("/publish-message",  async (req :  Request , res: Response) => {
  const rbmq = await rabbitMQConnection
  const channel = await rbmq.createChannel()
  try {
    const mockObjects = buildMockData({size: 100})
    
    mockObjects.sort(({time : t1, time :t2}) => t2-t1).map((el, i) => {
      el.data.map((K) => {
        console.log(JSON.stringify(K))
        channel.sendToQueue(`first-test-queue-${i}`, Buffer.from(JSON.stringify(K)), {
          persistent: true,    
          messageId: uuidv4(),      
          timestamp: Date.now()
        })
      })
    })
      
    
    
    res.sendStatus(200)
  }catch(err){
    console.log("Err",err) ,
    res.sendStatus(400)
  }finally{
    console.log("RabbitMq channel Closed")
    await channel.close()
    // await rbmq.close()
  }
})

app.get("/listen-message",  async (req :  Request , res: Response) => {  
  const rbmq = await rabbitMQConnection
  const channel = await rbmq.createChannel()
  // const els = []
  
  // channel.ackAll()

  

  try {

    Array(100).fill(null).map((_, i) =>{
      channel.assertQueue(`first-test-queue-${i}`).then(() => {
        channel.consume(`first-test-queue-${i}`, (msg) => {
          console.log(" Message received" ,JSON.parse(msg.content.toString()), msg.fields, msg.properties)
          channel.ack(msg)
        })
      })
    })
    
    // console.log("Listening for 20 seconds")
    setTimeout(async () => {
      res.sendStatus(200)
      channel.close()
      // rbmq.close()
    }, 60000)
  }catch(err){
    console.log({err})
  }finally{
    console.log("Control in timeout")
  }
})


app.listen(port, host, () => {
  console.log(`[ ready ] http://${host}:${port}`);
});



