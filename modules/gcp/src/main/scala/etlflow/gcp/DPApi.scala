package etlflow.gcp

import com.google.cloud.dataproc.v1.Cluster
import etlflow.model.Executor.DATAPROC
import zio.ZIO

object DPApi {
  trait Service[F[_]] {
    def createDataproc(config: DATAPROC, props: DataprocProperties): F[Cluster]
    def deleteDataproc(config: DATAPROC): F[Unit]
    def executeSparkJob(args: List[String], main_class: String, libs: List[String], config: DATAPROC): F[Unit]
    def executeHiveJob(query: String, config: DATAPROC): F[Unit]
  }

  def createDataproc(config: DATAPROC, props: DataprocProperties): ZIO[DPEnv, Throwable, Cluster] =
    ZIO.accessM(_.get.createDataproc(config, props))
  def deleteDataproc(config: DATAPROC): ZIO[DPEnv, Throwable, Unit] = ZIO.accessM(_.get.deleteDataproc(config))
  def executeSparkJob(
      args: List[String],
      main_class: String,
      libs: List[String],
      config: DATAPROC
  ): ZIO[DPEnv, Throwable, Unit] =
    ZIO.accessM(_.get.executeSparkJob(args, main_class, libs, config))
  def executeHiveJob(query: String, config: DATAPROC): ZIO[DPEnv, Throwable, Unit] =
    ZIO.accessM(_.get.executeHiveJob(query, config))
}
