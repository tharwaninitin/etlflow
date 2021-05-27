package etlflow.etlsteps

import etlflow.blobstore.Store
import etlflow.blobstore.GcsStore
import etlflow.blobstore.FileStore
import etlflow.blobstore.S3Store
import etlflow.blobstore.url.{Authority, FsObject, Path, Url}
import etlflow.aws.S3CustomClient
import etlflow.gcp.GCS
import etlflow.utils.Location
import fs2.{Pipe, Stream}
import zio.Runtime.default.unsafeRun
import zio.{Task}
import zio.interop.catz._
import zio.interop.catz.implicits._
import cats.syntax.all._

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
    etl_logger.info("#" * 50)

    etl_logger.info(s"Starting Sync Step: $name")

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
        unsafeRun(S3Store[Task](storage))
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
        unsafeRun(S3Store[Task](storage))
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
                etl_logger.error(ex.getMessage)
                etl_logger.error(ex.getStackTrace.mkString("\n"))
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
  } *> Task(etl_logger.info("#" * 50))
}