package etlflow.gcp

import com.google.cloud.dataproc.v1.Cluster
import etlflow.model.Executor.DATAPROC
import zio.ZIO

object DPApi {
  trait Service[F[_]] {
    def createDataproc(config: DATAPROC, props: DataprocProperties): F[Cluster]
    def deleteDataproc(config: DATAPROC): F[Unit]
  }

  def createDataproc(config: DATAPROC, props: DataprocProperties): ZIO[DPEnv, Throwable, Cluster] =
    ZIO.accessM(_.get.createDataproc(config, props))
  def deleteDataproc(config: DATAPROC): ZIO[DPEnv, Throwable, Unit] = ZIO.accessM(_.get.deleteDataproc(config))
}
