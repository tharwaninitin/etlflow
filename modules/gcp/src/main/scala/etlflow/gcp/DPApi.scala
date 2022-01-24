package etlflow.gcp

import com.google.cloud.dataproc.v1.Cluster
import etlflow.model.Executor.DATAPROC
import zio.{Task, ZIO}

object DPApi {
  trait Service {
    def createDataproc(config: DATAPROC, props: DataprocProperties): Task[Cluster]
    def deleteDataproc(config: DATAPROC): Task[Unit]
    def executeSparkJob(args: List[String], main_class: String, libs: List[String], config: DATAPROC): Task[Unit]
    def executeHiveJob(query: String, config: DATAPROC): Task[Unit]
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
