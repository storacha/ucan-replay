import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";
import { format } from "date-fns";

/**
 * Clears out all logs for a range of days
 * @param {string} bucket
 * @param {string} region
 * @param {Date[]} days
 */
export default async function clearS3Days(bucket, region, days) {
  // Create an S3 client
  const s3Client = new S3Client({ region });

  /**
   * Function to return all subdirectories in an S3 bucket
   * @param {*} bucket
   * @returns
   */
  async function listSubdirectories(bucket, prefix) {
    // List all objects in the bucket
    const listCommand = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      Delimiter: "/", // This will group objects by common prefixes
    });
    const listResponse = await s3Client.send(listCommand);

    if (
      !listResponse.CommonPrefixes ||
      listResponse.CommonPrefixes.length === 0
    ) {
      console.log("No subdirectories found in the bucket.");
      return;
    }

    // Extract the subdirectories
    return listResponse.CommonPrefixes.map((item) => item.Prefix);
  }

  // Function to delete all files in a directory
  async function deleteFilesInDirectory(bucket, prefix) {
    try {
      // List all objects in the directory

      let objectsToDelete = [];
      let listResponse = null;
      let continuationToken;
      do {
        const listCommand = new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken
        });
  
        listResponse = await s3Client.send(listCommand);

        if (listResponse.KeyCount === 0 || listResponse.Contents.length === 0) {
          console.log("No files found in the directory.");
          return;
        }

        // Prepare the objects to delete
        objectsToDelete = objectsToDelete.concat(
          listResponse.Contents.map((item) => ({ Key: item.Key }))
        );
        continuationToken = listResponse.NextContinuationToken
      } while (listResponse.IsTruncated);

      // Delete the objects
      const deleteCommand = new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
          Objects: objectsToDelete,
        },
      });
      const deleteResponse = await s3Client.send(deleteCommand);

      console.log("Deleted files:", deleteResponse.Deleted.map(item => item.Key));
    } catch (error) {
      console.error("Error deleting files:", error);
    }
  }

  const topSubDirectories = await listSubdirectories(bucket, "logs/");
  for (const topSubDirectory of topSubDirectories) {
    const subDirectories = await listSubdirectories(bucket, topSubDirectory);
    for (const subDirectory of subDirectories) {
      for (const day of days) {
        const dayString = format(day, "yyyy-MM-dd");
        console.log(`${subDirectory}${dayString}/`);
        await deleteFilesInDirectory(bucket, `${subDirectory}${dayString}/`);
      }
    }
  }
}
