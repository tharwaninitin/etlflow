package etlflow

import com.google.cloud.bigquery
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.dataproc.v1.{Cluster, Job}
import etlflow.log.ApplicationLogger
import gcp4zio.dp
import gcp4zio.dp.{DPCluster, DPJob}
import zio.{RIO, Task, TaskLayer, URIO, ZIO, ZLayer}
import java.time.Duration

package object gcp extends ApplicationLogger {

  sealed trait Location
  object Location {
    final case class LOCAL(path: String)               extends Location
    final case class GCS(bucket: String, path: String) extends Location
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def logBQJobs[R](effect: RIO[R, bigquery.Job]): URIO[R, Unit] =
    effect.fold(
      e => logger.error(e.getMessage),
      op => {
        // logger.info(s"EmailId: ${op.getUserEmail}")
        val stats = op.getStatistics.asInstanceOf[QueryStatistics]
        logger.info(s"${stats.getDmlStats}")
      }
    )

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
