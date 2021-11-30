package etlflow.gcp

import com.google.cloud.dataproc.v1.Cluster
import zio.{Task, ZIO}

private[etlflow] object DPApi {
  trait Service {
    def executeSparkJob(args: List[String], main_class: String, libs: List[String]): Task[Unit]
    def executeHiveJob(query: String): Task[Unit]
    def createDataproc(props: DataprocProperties): Task[Cluster]
    def deleteDataproc(): Task[Unit]
  }
  def executeSparkJob(args: List[String], main_class: String, libs: List[String]): ZIO[DPEnv, Throwable, Unit] = ZIO.accessM(_.get.executeSparkJob(args, main_class, libs))
  def executeHiveJob(query: String): ZIO[DPEnv, Throwable, Unit] = ZIO.accessM(_.get.executeHiveJob(query))
  def createDataproc(props: DataprocProperties): ZIO[DPEnv, Throwable, Cluster] = ZIO.accessM(_.get.createDataproc(props))
  def deleteDataproc(): ZIO[DPEnv, Throwable, Unit] = ZIO.accessM(_.get.deleteDataproc())
}
