package etlflow.gcp

import etlflow.model.Executor.DATAPROC
import zio.ZIO

object DPJobApi {
  trait Service[F[_]] {
    def executeSparkJob(args: List[String], main_class: String, libs: List[String], config: DATAPROC): F[Unit]
    def executeHiveJob(query: String, config: DATAPROC): F[Unit]
  }

  def executeSparkJob(
      args: List[String],
      main_class: String,
      libs: List[String],
      config: DATAPROC
  ): ZIO[DPJobEnv, Throwable, Unit] =
    ZIO.accessM(_.get.executeSparkJob(args, main_class, libs, config))
  def executeHiveJob(query: String, config: DATAPROC): ZIO[DPJobEnv, Throwable, Unit] =
    ZIO.accessM(_.get.executeHiveJob(query, config))
}
