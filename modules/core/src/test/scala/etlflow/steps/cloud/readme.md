# SCENARIO 1) Analyse BigQuery query logs (Realtime and Batch)
In below couple of examples we will create pipelines to analyse the big query logs to get the information about the query.

Google Cloud Pub/Sub is a fully-managed real-time messaging service that allows you to send and receive messages between independent applications. Pub/Sub is an asynchronous messaging service that decouples services that produce events from services that process events. You can use Pub/Sub as messaging-oriented middleware or event ingestion and delivery for streaming analytics pipelines. Pub/Sub offers durable message storage and real-time message delivery with high availability and consistent performance at scale. Pub/Sub servers run in all Google Cloud regions around the world.

Prerequisite: 
   * PubSub topic should be created in GCP which should have incoming stream of BiqQuery audit logs for Realtime pipeline.
   * PubSub subscription should be created which will be consumed by our GCSPubSubStep.
   * BigQuery log exports to GCS should be created for Batch pipeline.
   * We will only store logs with json field having methodName="jobservice.jobcompleted".

# Libraries Used
| Library Name| Description | Artifact Name |
| :---        |    :----    |     :---:     |
| **fs2**     | [FS2](https://fs2.io/) is a library for purely functional, effectful, and polymorphic stream processing library in the Scala programming language. FS2 is built upon two major functional libraries for Scala, [Cats](https://typelevel.org/cats/) and [Cats-Effect](https://typelevel.org/cats-effect/).       | "co.fs2" %% "fs2-core" % {version}   |
| **circe-optics**   | [Optics](https://circe.github.io/circe/optics.html) are a powerful tool for traversing and modifying JSON documents. They can reduce boilerplate considerably, especially if you are working with deeply nested JSON.        | "io.circe" %% "circe-optics" % {version}    |
| **skunk** | [Skunk](https://tpolecat.github.io/skunk/) is a Postgres library for Scala. It is powered by cats, cats-effect, scodec, and fs2. It is purely functional, non-blocking, and provides a tagless-final API. Also it gives very good error messages. |  "org.tpolecat" %% "skunk-core" % {version} |
| **fs2-blobstore**     |    [fs2-blobstore](https://github.com/fs2-blobstore/fs2-blobstore): Minimal, idiomatic, Scala interface based on fs2 for blob- and file store implementations. This library provides sources as sinks for a variety of stores (S3, GCS, Azure, SFTP, Box) that you can compose with fs2 programs.   |     "com.github.fs2-blobstore" %% "gcs" % {version}    |
| **testcontainers**| [testcontainers](https://github.com/testcontainers/testcontainers-java) is a library that allows using docker containers for functional/integration testing. | "org.testcontainers" %% "postgresql" % {version} % "test" |

Note: All below tests are integration tests. That is, they make real API requests to GCS, AWS.

## 1) Batch pipeline from GCS to Database
   We will follow below steps to create the pipeline.
   * We will fetch BigQuery logs stored in GCS which contain files having events in json format. 
   * Carry out transformation in fs2-stream to create QueryMetrics from json string (see *jsonParser* inside *DbHelper.scala*). 
   * Populate the resulted data generated after the transformation into the database.

#### For testing CloudStoreTestSuite follow below instructions
	export GOOGLE_APPLICATION_CREDENTIALS=<...> # this should be full path to GCP Service Account Key Json which should have PUBSUB Read access 
	export GCS_INPUT_LOCATION=<...>
	
	sbt "project core" testOnly etlflow.steps.cloud.CloudStoreTestSuite

## 2) Realtime Streaming pipeline from PubSub to Database   
   We will follow below steps to create the pipeline.
   * Pull messages published to a Pub/Sub topic using pull subscriber which contain events in json format. 
   * Carry out transformation in fs2-stream to create QueryMetrics from json string (see *jsonParser* inside *DbHelper.scala*). 
   * Populate the resulted data generated after the transformation into the database.
 	
#### For testing GCSPubSubStepTestSuite follow below instructions
	export GOOGLE_APPLICATION_CREDENTIALS=<...> # this should be full path to GCP Service Account Key Json which should have GCS Read access 
	export PUBSUB_SUBSCRIPTION=<...>
	export GCP_PROJECT_ID=<...>
    
    sbt "project core" testOnly etlflow.steps.cloud.GCSPubSubStepTestSuite
