package etlflow.etlsteps

import gcp4zio._
import zio.{RIO, Task, ZIO}

case class GCSDeleteStep(
    name: String,
    bucket: String,
    prefix: String,
    parallelism: Int
) extends EtlStep[GCSEnv, Unit] {

  override def process: RIO[GCSEnv, Unit] = {
    logger.info(s"Deleting files at $bucket/$prefix")
    for {
      list <- GCSApi.listObjects(bucket, prefix)
      _ <- ZIO.foreachParN_(parallelism)(list)(blob =>
        Task {
          logger.info(s"Deleting object ${blob.getName}")
          blob.delete()
        }
      )
    } yield ()
  }

  override def getStepProperties: Map[String, String] = Map(
    "bucket"      -> bucket,
    "prefix"      -> prefix,
    "parallelism" -> parallelism.toString
  )
}
