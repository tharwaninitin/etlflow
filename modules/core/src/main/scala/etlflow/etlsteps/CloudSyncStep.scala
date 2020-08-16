package etlflow.etlsteps

import blobstore.Path
import blobstore.gcs.GcsStore
import cats.effect.Blocker
import etlflow.gcp.GCSClient
import etlflow.utils.Environment
import zio.{Task, UIO}
import zio.interop.catz._

case class CloudSyncStep(
           name: String,
           inputPath: String,
           creds: Option[Environment.GCP] = None,
           outputPath: String,
         )
  extends EtlStep[Unit,Unit] {
  final def process(input: => Unit): Task[Unit] = {
    Task.concurrentEffectWith { implicit CE =>
      Blocker[Task].use { blocker =>
        val storage = GCSClient(creds)

        etl_logger.info("#"*50)
        etl_logger.info(s"Starting Sync Step: $name")

        val inputStore = GcsStore[Task](storage, blocker, List.empty)
        val outputStore = GcsStore[Task](storage, blocker, List.empty)

        inputStore.get(Path(inputPath), 4096)
          .through(outputStore.put(Path(outputPath)))
          .compile.drain
      }
    } *> Task(etl_logger.info("#"*50))
  }
}