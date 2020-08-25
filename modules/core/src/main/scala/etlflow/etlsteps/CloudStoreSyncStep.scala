package etlflow.etlsteps

import java.nio.file.Paths

import blobstore.fs.FileStore
import blobstore.gcs.GcsStore
import blobstore.s3.S3Store
import blobstore.{Path, Store}
import cats.effect.Blocker
import etlflow.aws.S3CustomClient
import etlflow.gcp.GCSClient
import etlflow.utils.Location
import fs2.{Pipe, Stream, text}
import zio.Runtime.default.unsafeRun
import zio.Task
import zio.interop.catz._

case class CloudStoreSyncStep (
       name: String,
       input_location: Location,
       output_location: Location,
       transformation: Option[Pipe[Task,Byte,Byte]] = None,
       parallelism: Int = 1
     )
  extends EtlStep[Unit,Unit] {

  final def process(input: => Unit): Task[Unit] = {
    Task.concurrentEffectWith { implicit CE =>
      Blocker[Task].use { blocker =>

        etl_logger.info("#"*50)
        etl_logger.info(s"Starting Sync Step: $name")

        val inputStore: Store[Task] = input_location match {
          case location: Location.GCS =>
            val storage = GCSClient(location)
            GcsStore[Task](storage, blocker, List.empty)
          case location: Location.S3 =>
            val storage = S3CustomClient(location)
            unsafeRun(S3Store[Task](storage))
          case _: Location.LOCAL =>
            FileStore[Task](Paths.get(""), blocker)
          case _ => ???
        }

        val inputStorePath: Path = input_location match {
          case location: Location.LOCAL =>  Path(location.path)
          case location: Location.GCS   =>  Path(location.path)
          case location: Location.S3    =>  Path(location.path)
        }

        val outputStore: Store[Task] = output_location match {
          case location: Location.GCS =>
            val storage = GCSClient(location)
            GcsStore[Task](storage, blocker, List.empty)
          case location: Location.S3 =>
            val storage = S3CustomClient(location)
            unsafeRun(S3Store[Task](storage))
          case _: Location.LOCAL =>
            FileStore[Task](Paths.get(""), blocker)
        }

        val outputStorePath: Path = output_location match {
          case location: Location.LOCAL =>  Path(location.path)
          case location: Location.GCS   =>  Path(location.path)
          case location: Location.S3    =>  Path(location.path)
          case _ => ???
        }

        inputStore.list(inputStorePath)
          .map { path: Path =>

            val startMarkerStream = Stream.eval(Task(println(s"Starting to load file from $path with size ${path.size.getOrElse(0L)/1024.0} KB " +
              s"to ${outputStorePath + path.fileName.get}")))

            val inputStream     = inputStore.get(path, chunkSize = 32 * 1024)
            val transferStream  = transformation.map(trans => inputStream.through(trans)).getOrElse(inputStream)
            val outputStream    = transferStream.through(outputStore.put(Path(outputStorePath + path.fileName.get)))

            val doneMarkerStream = Stream.eval(Task(println(s"Done loading file $path")))

            val finalStream = startMarkerStream ++ outputStream ++ doneMarkerStream

            if (path.fileName.isDefined) {
              if (path.fileName.get.endsWith("/")) {
                Stream.empty
              } else {
                finalStream
              }
            } else {
              Stream.empty
            }
          }
          .parJoin(maxOpen = parallelism)
          .compile.drain

      }
    } *> Task(etl_logger.info("#"*50))
  }
}