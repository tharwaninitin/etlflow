package etlflow.etlsteps

import java.nio.file.Paths
import blobstore.fs.FileStore
import blobstore.gcs.GcsStore
import blobstore.s3.S3Store
import blobstore.{Path, Store}
import cats.effect.Blocker
import etlflow.aws.S3CustomClient
import etlflow.gcp.GCS
import etlflow.utils.Location
import fs2.{Pipe, Stream}
import zio.Runtime.default.unsafeRun
import zio.Task
import zio.interop.catz._

case class CloudStoreStep[T] (
       name: String,
       input_location: Location,
       transformation: Pipe[Task,Byte,Either[Throwable,T]],
       success_handler: T => Task[Unit],
       error_handler: Throwable => Task[Unit],
       parallelism: Int = 1,
       chunk_size: Int = 32 * 1024
     )
  extends EtlStep[Unit,Unit] {

  final def process(input: => Unit): Task[Unit] = {
    Task.concurrentEffectWith { implicit CE =>
      Blocker[Task].use { blocker =>

        etl_logger.info("#"*50)
        etl_logger.info(s"Starting Sync Step: $name")

        val inputStore: Store[Task] = input_location match {
          case location: Location.GCS =>
            val storage = GCS.getClient(location)
            GcsStore[Task](storage, blocker, List.empty)
          case location: Location.S3 =>
            val storage = S3CustomClient(location)
            unsafeRun(S3Store[Task](storage))
          case _: Location.LOCAL =>
            FileStore[Task](Paths.get(""), blocker)
        }

        val inputStorePath: Path = Path(input_location.location)

        inputStore.list(inputStorePath)
          .map { input_path: Path =>

            if (input_path.fileName.isDefined) {
              if (input_path.fileName.get.endsWith("/")) {
                Stream.empty
              } else {
                val startMarkerStream = Stream.eval(Task(println(s"Starting to load file from $input_path with size ${input_path.size.getOrElse(0L)/1024.0} KB")))
                val inputStream       = inputStore.get(input_path, chunk_size)
                val outputStream      = inputStream.through(transformation).flatMap {
                                          case Left(ex) => Stream.eval_(error_handler(ex))
                                          case Right(value) => Stream.eval(success_handler(value))
                                        }
                val doneMarkerStream  = Stream.eval(Task(println(s"Done loading file $input_path")))
                startMarkerStream ++ outputStream ++ doneMarkerStream
              }
            } else {
              Stream.empty
            }
          }
          .parJoin(parallelism)
          .compile.drain
      }
    } *> Task(etl_logger.info("#"*50))
  }
}