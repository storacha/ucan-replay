
import { eachDayOfInterval, endOfDay, startOfDay } from "date-fns";
import fs from 'fs'
import fsPromises from 'node:fs/promises'
import path from 'path'
import AsyncBuffer from "./asyncbuffer.js";
import generateKinesisRecords from "./kinesis.js";
import consumeKinesisToFirehose from "./firehose.js";
import clearS3Days from "./s3.js";
import { UTCDate } from "@date-fns/utc";
if (process.argv.length <= 3) {
  console.error('Expects at start date and end date (inclusive) as arguments');
  process.exit(1);
}

const startDate = new UTCDate(process.argv[2])
const startTime= startOfDay(startDate)
const endDate = new UTCDate(process.argv[3])
const endTime = endOfDay(endDate)
const days = eachDayOfInterval({start: startDate, end: endDate})
         
/**
 * @import {_Record} from "@aws-sdk/client-kinesis"
 */

/** @type {AsyncBuffer<import("@aws-sdk/client-kinesis")._Record>} */
const buffer = new AsyncBuffer(100, 10000000)

/** @type {(AsyncGenerator<_Record[]>) => Promise<void>)} */
var handleStream

let absoluteWriteDir
if (process.argv.length > 4) {
  const writeDir = process.argv[4]
  absoluteWriteDir = path.resolve(writeDir);
  if (!fs.existsSync(absoluteWriteDir)) {
    fs.mkdirSync(absoluteWriteDir, { recursive: true });
  }
}

if (process.argv.length === 5) {
  handleStream = 
  /**
  * 
  * @param {AsyncGenerator<_Record[]>} records 
  */
  async (records) => {
    for await (const batch of records) {
      for (const record of batch) {
        const filePath = `${absoluteWriteDir}/${record.SequenceNumber}`;
        fs.writeFileSync(filePath, record.Data); 
      }
    }
  }
  console.log(handleStream)
} else {
  await clearS3Days('stream-log-store-prod-0', 'us-west-2', days)
  
  handleStream = 
  /**
   * 
   * @param {AsyncGenerator<_Record[]>} records 
   */
  (records) => consumeKinesisToFirehose("hannah-w3infra-manual-stream-delivery-0", records)
}

/** @type {() => AsyncGenerator<_Record[]>} */
var generateStream
if (process.argv.length > 5 && process.argv[5] === 'local') {
  generateStream = async function * () {
    const dir = await fsPromises.opendir(absoluteWriteDir, { bufferSize: 100 })
    for await (const dirent of dir) {
      if (dirent.isFile()) {
        const filePath = path.join(absoluteWriteDir, dirent.name);
        let fileContent = await fsPromises.readFile(filePath, 'utf-8');
        const record = JSON.parse(fileContent);
        const uint8Array = Uint8Array.from(Buffer.from(fileContent, 'utf-8'));
        /** @type {_Record} */
        const data = { ApproximateArrivalTimestamp: new Date(record.ts), Data: uint8Array, SequenceNumber: dirent.name}
        yield [data]
        await fsPromises.rm(filePath)
      }
    }
  }
} else {
  generateStream = () => generateKinesisRecords("prod-w3infra-ucan-stream-v2", endTime, days)
}

await Promise.all([
  buffer.streamIn(generateStream()),
  handleStream(buffer.streamOut())
])


// console.log('Received event:', JSON.stringify(event, null, 2))

//   for (const record of event.Records) {
//     // Kinesis data is base64 encoded so decode here
//     const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8')
//     console.log('Decoded payload:', payload)

//     // Prepare the Firehose record
//     const firehoseRecord = {
//       DeliveryStreamName: 'YOUR_FIREHOSE_DELIVERY_STREAM_NAME', // Replace with your Firehose delivery stream name
//       Record: {
//         Data: payload + '\n', // Add a newline character to separate records
//       },
//     }

//     // Put the record into the Firehose delivery stream
//     try {
//       await firehose.putRecord(firehoseRecord).promise()
//       console.log('Record successfully put into Firehose:', firehoseRecord)
//     } catch (error) {
//       console.error('Error putting record into Firehose:', error)
//     }
//   }

//   return `Successfully processed ${event.Records.length} records.`
// }
