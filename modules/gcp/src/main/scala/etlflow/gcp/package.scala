package etlflow

import com.google.cloud.dataproc.v1.{Cluster, Job}
import etlflow.log.ApplicationLogger
import gcp4zio.dp
import gcp4zio.dp.{DPCluster, DPJob}
import zio.{Task, TaskLayer, ZIO, ZLayer}
import java.time.Duration

package object gcp extends ApplicationLogger {

  sealed trait Location
  object Location {
    final case class LOCAL(path: String)               extends Location
    final case class GCS(bucket: String, path: String) extends Location
  }

  val dbJobNoopLayer: TaskLayer[DPJob] = ZLayer.succeed(new DPJob {
    override def submitSparkJob(args: List[String], mainClass: String, libs: List[String], conf: Map[String, String]): Task[Job] =
      ZIO.fail(new RuntimeException(""))
    override def submitHiveJob(query: String): Task[Job]                    = ZIO.fail(new RuntimeException(""))
    override def trackJobProgress(job: Job, interval: Duration): Task[Unit] = ZIO.fail(new RuntimeException(""))
  })

  val dbClusterNoopLayer: TaskLayer[DPCluster] = ZLayer.succeed(new DPCluster {
    override def createDataproc(cluster: String, props: dp.ClusterProps): Task[Cluster] = ZIO.fail(new RuntimeException(""))
    override def deleteDataproc(cluster: String): Task[Unit]                            = ZIO.fail(new RuntimeException(""))
  })
}
