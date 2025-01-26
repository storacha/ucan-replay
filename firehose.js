
import {
  FirehoseClient,
  FirehoseServiceException,
  InvalidArgumentException,
  PutRecordBatchCommand,
} from "@aws-sdk/client-firehose";



/**
 * @param {string} firehoseName
 * @param {AsyncGenerator<import("@aws-sdk/client-kinesis")._Record[]} recordsStream 
 */
export default async function consumeKinesisToFirehose(firehoseName, recordsStream) {

const firehoseClient = new FirehoseClient();

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
 * @param {[import("@aws-sdk/client-kinesis")._Record]} records 
 * @param {number} chunkSize 
 * @param {number} split
 */
async function putRecordsRecursiveSize(records, chunkSize, split) {
  const recordChunks = chunkArray(records, chunkSize);
  for (const chunk of recordChunks) {
    const formattedRecords = chunk.map((record) => {
      const payload = Buffer.from(record.Data, "base64").toString("utf-8");
      return {
        Data: new TextEncoder().encode(payload), // Add a newline character to separate records
      };
    });
    // Prepare the Firehose record
    const putRecordBatchCommand = new PutRecordBatchCommand({
      DeliveryStreamName: firehoseName,
      Records: formattedRecords,
    });
    try {
      await firehoseClient.send(putRecordBatchCommand);
    } catch (e) {
      if (e instanceof InvalidArgumentException || e instanceof FirehoseServiceException) {
        if (chunkSize > 1) {
          console.log(`Reducing chunk size to ${ Math.ceil(chunkSize / split) }`)
          await putRecordsRecursiveSize(records, Math.ceil(chunkSize / split), split)
        } else {
          console.warn("unable to put record", "validation error", e)
        }
      } else {
        console.log(e.message)
        throw e
      }
    }
  }
}

  for await (const batch of recordsStream) {
    console.log(
      "Processing next batch",
      "starting date",
      batch[0].ApproximateArrivalTimestamp
    ); 
    await putRecordsRecursiveSize(batch, 100, 5)
    console.log(`Processed ${batch.length} records`);
  }
}