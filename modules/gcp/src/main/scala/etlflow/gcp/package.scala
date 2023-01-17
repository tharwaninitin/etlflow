package etlflow

import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import etlflow.log.ApplicationLogger
import zio.{RIO, URIO}

package object gcp extends ApplicationLogger {

  sealed trait Location
  object Location {
    final case class LOCAL(path: String)               extends Location
    final case class GCS(bucket: String, path: String) extends Location
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def logs[R](effect: RIO[R, Job]): URIO[R, Unit] =
    effect.fold(
      e => logger.error(e.getMessage),
      op => {
        // logger.info(s"EmailId: ${op.getUserEmail}")
        val stats = op.getStatistics.asInstanceOf[QueryStatistics]
        logger.info(s"${stats.getDmlStats}")
      }
    )
}
