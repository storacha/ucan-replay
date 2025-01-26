import {
  DescribeStreamCommand,
  DescribeStreamConsumerCommand,
  GetRecordsCommand,
  GetShardIteratorCommand,
  KinesisClient,
  KinesisServiceException,
  LimitExceededException,
  ListShardsCommand,
  ListStreamConsumersCommand,
  ProvisionedThroughputExceededException,
  RegisterStreamConsumerCommand,
  ResourceNotFoundException,
  ShardIteratorType,
  SubscribeToShardCommand,
} from "@aws-sdk/client-kinesis";
import { Repeater } from "@repeaterjs/repeater";
import { addHours, endOfDay, format, startOfDay } from "date-fns";

/**
 * @function
 * @template T
 * @param {[T]} array
 * @param {number} chunkSize
 */
function chunkArray(array, chunkSize) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}


/**
 * 
 * @param {AsyncIterable<SubscribeToShardEventStream>} shardEvents 
 */
async function * consumeShardEventStream(shardEvents) {
  for await (const shardEvent of shardEvents) {
    if (shardEvent.SubscribeToShardEvent) {
      yield shardEvent.SubscribeToShardEvent
    } else {
      console.log("warning: received non standard shard event", shardEvent)
    }
  }
}

/**
 * 
 * @param {string} streamARN 
 * @param {number} n 
 * @param {KinesisClient} kinesisClient 
 */
const setupConsumer = async (streamARN, n, kinesisClient) => {
  let consumerARN
  const describeStreamConsumerCommand = new DescribeStreamConsumerCommand({
    StreamARN: streamARN,
    ConsumerName: `${consumerName}-${n.toString()}`
  })
  try {
    const consumerResponse = await kinesisClient.send(describeStreamConsumerCommand)
    consumerARN = consumerResponse.ConsumerDescription?.ConsumerARN
  } catch (err) {
    if (err instanceof ResourceNotFoundException) {
      const registerStreamConsumerCommand = new RegisterStreamConsumerCommand({
        StreamARN: streamARN,
        ConsumerName: `${consumerName}-${n.toString()}`
      })
      let createConsumerResponse
      for (;;) {
        try {
          createConsumerResponse = await kinesisClient.send(registerStreamConsumerCommand)
        break
        } catch (err) {
          if (err instanceof LimitExceededException) {
            await new Promise((resolve) => setTimeout(resolve, 800));
            continue
          } else {
            throw err
          }
        }
      }
      consumerARN = createConsumerResponse.Consumer?.ConsumerARN
      if (consumerARN == undefined) {
        throw new Error("did not establish a consumer ARN")
      }
      for (;;) {
        const consumerResponse = await kinesisClient.send(describeStreamConsumerCommand)
        if (consumerResponse.ConsumerDescription.ConsumerStatus !== 'ACTIVE') {
          await new Promise((resolve) => setTimeout(resolve, 800));
        } else {
          break
        }
      }
    } else {
      throw err
    }
  }

  console.log(`successfully registered stream ${n.toString()}`)
  return consumerARN
}

async function * consumeTimeRange(consumerARN, shardId, startTime, endTime, kinesisClient) {
  let subscribeToShardCommand = new SubscribeToShardCommand({
    ConsumerARN: consumerARN,
    ShardId: shardId,
    StartingPosition: { // StartingPosition
      Type: ShardIteratorType.AT_TIMESTAMP,
      Timestamp: startTime
    },
  })

  let sequenceNumber
  for (;;) {
    
    let subscribeResponse = await kinesisClient.send(subscribeToShardCommand)

    if (subscribeResponse.EventStream === undefined) {
      throw new Error("unable to subscribe to consumer")
    }

    const recordStream = consumeShardEventStream(subscribeResponse.EventStream)
    try {
      for await (const records of recordStream) {
        console.log(
          "Read batch",
          "starting date",
          records.Records[0].ApproximateArrivalTimestamp,
          "record count",
          records.Records.length
        ); 
        if (records.Records.at(-1).ApproximateArrivalTimestamp > endTime) {
          const endIndex = records.Records.findIndex((record) => record.ApproximateArrivalTimestamp > endTime)
          yield records.Records.slice(0, endIndex)
          return
        }
        yield records.Records
        sequenceNumber = records.Records.at(-1).SequenceNumber
      }
    } catch (err) {
      console.log("warning: err processing event stream", err)
    }
    subscribeToShardCommand = new SubscribeToShardCommand({
      ConsumerARN: consumerARN,
      ShardId: shardId,
      StartingPosition: { // StartingPosition
        Type: ShardIteratorType.AFTER_SEQUENCE_NUMBER,
        SequenceNumber: sequenceNumber
      },
    })
  }
}

async function * executeConsumer(consumerARN, i, shardId, days, kinesisClient) {
  const startTime = startOfDay(days[0])
  for (;i < days.length*24; i+=20) {
    yield *consumeTimeRange(consumerARN, shardId, addHours(startTime, i), addHours(startTime, i+1), kinesisClient)   
  }
}

/**
 * @import { _Record, SubscribeToShardEventStream } from '@aws-sdk/client-kinesis'
 */
const consumerName = 'ucanReplay'
/**
 * 
 * @param {string} streamName 
 * @param {Date} endTime 
 * @param {Date[]} days
 */
export default async function * generateKinesisRecords(streamName, endTime, days) {

  const kinesisClient = new KinesisClient({});

const describeStreamCommand = new DescribeStreamCommand({
  StreamName: streamName,
});
const response = await kinesisClient.send(describeStreamCommand);

const shardId = response.StreamDescription.Shards[0].ShardId;
const streamARN = response.StreamDescription.StreamARN

const consumerNumbers = [...Array(20).keys()]
const consumerARNPromises = consumerNumbers.map((n) => setupConsumer(streamARN, n, kinesisClient))

const consumerARNs = await Promise.all(consumerARNPromises)

const recordStreams = consumerARNs.map((consumerARN, i) => executeConsumer(consumerARN, i, shardId, days, kinesisClient))

yield * Repeater.merge(recordStreams)
}
/*

const iteratorCommand = new GetShardIteratorCommand({
  StreamName: streamName,
  ShardId: shardId,
  ShardIteratorType: ShardIteratorType.AT_TIMESTAMP,
  Timestamp: startTime
});

let iterator = (await kinesisClient.send(iteratorCommand)).ShardIterator;

/**
 * Reads next set of records from kinesis with recorver for provision limits
 * 
 * @param {() => Promise<import("@aws-sdk/client-kinesis").GetRecordsCommandOutput>} getNextRecords
 * @param {number} retries
 * @param {number} delayMs
 */
const getRecords = async (getNextRecords, retries, delayMs) => {
  for (let i = 0; i < retries; i++) {
    try {
      const records = await getNextRecords()
      return records
    } catch (e) {
      if (e instanceof ProvisionedThroughputExceededException || e instanceof KinesisServiceException) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      } else {
        throw e;
      }
    }
  }
  throw new Error("too many retries on GetRecords");
}

/**
 * Reads kinesis records until a non-empty set is found
 * @param {number} limitTries 
 * @param {string} iterator
 * @param {KinesisClient} kinesisClient
 * @returns 
 */
const nextNonEmptyRecords = (limitTries, iterator, kinesisClient) => {
  return async () => {
    for (let i = 0; i < limitTries; i++) {
      const getRecordsCommand = new GetRecordsCommand({
        ShardIterator: iterator,
        Limit: 10000,
      });
    
      const records = await kinesisClient.send(getRecordsCommand);
      iterator = records.NextShardIterator  
      if (records.Records.length > 0) {
        return records;
      }
    }
    throw new Error("too many empty records on GetRecords")
  }
}

/**
 * 
 * @param {string} iterator 
 * @param {KinesisClient} kinesisClient 
 * @returns 
 */
const nextNonEmptyRecords1000 = (iterator, kinesisClient) => nextNonEmptyRecords(1000, iterator, kinesisClient) 