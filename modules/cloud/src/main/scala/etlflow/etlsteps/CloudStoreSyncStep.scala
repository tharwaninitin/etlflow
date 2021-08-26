package etlflow.etlsteps

import blobstore.Store
import blobstore.fs.FileStore
import blobstore.gcs.GcsStore
import blobstore.s3.S3Store
import blobstore.url.{Authority, FsObject, Path, Url}
import cats.syntax.all._
import etlflow.aws.S3CustomClient
import etlflow.gcp.GCS
import etlflow.schema.Location
import fs2.{Pipe, Stream}
import zio.Task
import zio.interop.catz._
import zio.interop.catz.implicits._

case class CloudStoreSyncStep (
       name: String,
       input_location: Location,
       output_location: Location,
       transformation: Option[Pipe[Task,Byte,Byte]] = None,
       output_overwrite: Boolean = false,
       parallelism: Int = 1,
       chunk_size: Int = 32 * 1024
     )
  extends EtlStep[Unit,Unit] {

  def getBucketInfo(bucket: String): Authority = Authority.unsafe(bucket)

  final def process(input: => Unit): Task[Unit] = {
    logger.info("#" * 50)

    logger.info(s"Starting Sync Step: $name")

    val inputBucket: Authority = getBucketInfo(input_location.bucket)
    val outputBucket: Authority = getBucketInfo(output_location.bucket)

    var inputStorePath: Url[String] = null
    var outputStorePath: Url[String] = null
    var output_scheme:String = null   // default value. This var will get set below

    val inputStore: Store[Task, FsObject] = input_location match {
      case location: Location.GCS =>
        val storage = GCS.getClient(location)
        inputStorePath = Url("gs", inputBucket, Path(input_location.location))
        GcsStore[Task](storage, List.empty)
      case location: Location.S3 =>
        val storage = S3CustomClient(location)
        inputStorePath = Url("s3", inputBucket, Path(input_location.location))
        S3Store[Task](storage)
      case _: Location.LOCAL =>
        inputStorePath = Url("file", Authority.localhost, Path(input_location.location))
        FileStore[Task].lift((u: Url[String]) => u.path.valid)
    }

    val outputStore: Store[Task, FsObject] = output_location match {
      case location: Location.GCS =>
        val storage = GCS.getClient(location)
        outputStorePath = Url("gs", outputBucket, Path(output_location.location))
        output_scheme = "gs"
        GcsStore[Task](storage, List.empty)
      case location: Location.S3 =>
        val storage = S3CustomClient(location)
        outputStorePath = Url("s3", outputBucket, Path(output_location.location))
        output_scheme = "s3"
        S3Store[Task](storage)
      case _: Location.LOCAL =>
        outputStorePath = Url("file", Authority.localhost, Path(output_location.location))
        output_scheme = "file"
        FileStore[Task].lift((u: Url[String]) => u.path.valid)
    }

    inputStore.list(inputStorePath, true)
      .map { input_path =>
        if (input_path.path.fileName.isDefined) {
          if (input_path.path.fileName.get.endsWith("/")) {
            Stream.empty
          } else {
            val output_path = Url(output_scheme, outputBucket, Path(output_location.location  + input_path.path.fileName.get))
            val startMarkerStream = Stream.eval(Task(println(s"Starting to load file from $input_path with size ${input_path.path.size.getOrElse(0L) / 1024.0} KB to $output_path")))
            val inputStream = inputStore.get(input_path, chunk_size)
            val transferStream = transformation.map(trans => inputStream.through(trans)).getOrElse(inputStream)
            val outputStream = transferStream.through(outputStore.put(output_path, output_overwrite))
              .handleErrorWith { ex =>
                logger.error(ex.getMessage)
                logger.error(ex.getStackTrace.mkString("\n"))
                Stream.raiseError[Task](ex)
              }
            val doneMarkerStream = Stream.eval(Task(println(s"Done loading file $input_path")))
            startMarkerStream ++ outputStream ++ doneMarkerStream
          }
        }
        else {
          Stream.empty
        }
      }
      .parJoin(maxOpen = parallelism)
      .compile.drain

    Task(logger.info("#" * 50))
  } *> Task(logger.info("#" * 50))
}