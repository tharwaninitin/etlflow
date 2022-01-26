package etlflow.etlsteps

import blobstore.Store
import blobstore.fs.FileStore
import blobstore.gcs.GcsStore
import blobstore.s3.S3Store
import blobstore.url.{Authority, FsObject, Path, Url}
import cats.syntax.all._
import etlflow.aws.S3Client
import etlflow.cloud.{getBucketInfo, Location}
import etlflow.gcp.GCSClient
import fs2.{Pipe, Stream}
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{RIO, Task}
import zio.interop.catz._
import zio.interop.catz.implicits._

case class CloudStoreStep[T](
    name: String,
    input_location: Location,
    transformation: Pipe[Task, Byte, Either[Throwable, T]],
    success_handler: T => Task[Unit],
    error_handler: Throwable => Task[Unit],
    parallelism: Int = 1,
    chunk_size: Int = 32 * 1024
) extends EtlStep[Clock with Blocking, Unit] {

  final def process: RIO[Clock with Blocking, Unit] = {
    logger.info("#" * 50)
    logger.info(s"Starting Sync Step: $name")

    val inputBucket: Authority = getBucketInfo(input_location.bucket)
    var inputStorePath         = Url("gs", inputBucket, Path(input_location.location)) //

    val inputStore: Store[Task, FsObject] = input_location match {
      case location: Location.GCS =>
        val storage = GCSClient(location.credentials)
        inputStorePath = Url("gs", inputBucket, Path(input_location.location))
        GcsStore[Task](storage, List.empty)
      case location: Location.S3 =>
        val storage = S3Client(Region.of(location.region), location.credentials)
        inputStorePath = Url("s3", inputBucket, Path(input_location.location))
        S3Store[Task](storage)
      case _: Location.LOCAL =>
        inputStorePath = Url("file", getBucketInfo("localhost"), Path(input_location.location))
        FileStore[Task].lift((u: Url[String]) => u.path.valid)
    }

    inputStore
      .list(inputStorePath)
      .map { input_path =>
        if (input_path.path.fileName.isDefined) {
          if (input_path.path.fileName.get.endsWith("/")) {
            Stream.empty
          } else {
            val startMarkerStream = Stream.eval(
              Task(
                println(
                  s"Starting to load file from $input_path with size ${input_path.path.size.getOrElse(0L) / 1024.0} KB"
                )
              )
            )
            val inputStream = inputStore.get(input_path, chunk_size)
            val outputStream = inputStream.through(transformation).flatMap {
              case Left(ex)     => Stream.eval(error_handler(ex))
              case Right(value) => Stream.eval(success_handler(value))
            }
            val doneMarkerStream = Stream.eval(Task(println(s"Done loading file $input_path")))
            startMarkerStream ++ outputStream ++ doneMarkerStream
          }
        } else {
          Stream.empty
        }
      }
      .parJoin(parallelism)
      .compile
      .drain
  } *> Task(logger.info("#" * 50))
}
