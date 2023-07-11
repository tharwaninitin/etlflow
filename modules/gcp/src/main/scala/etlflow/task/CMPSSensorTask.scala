package etlflow.task

import com.google.monitoring.v3.TimeInterval
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import gcp4zio.monitoring.Monitoring
import zio.{RIO, ZIO}
import scala.concurrent.duration.Duration

/** Cloud Monitoring sensor on Pubsub Subscription to check for num_undelivered_messages
  *
  * @param name
  *   Name for the task
  * @param project
  *   GCP Project ID
  * @param subscription
  *   PubSub Subscription ID
  * @param interval
  *   Cloud Monitoring Interval with start time and end time.
  * @param spaced
  *   Check continuously with every repetition spaced by the specified duration from the last run.
  */
case class CMPSSensorTask(name: String, project: String, subscription: String, interval: TimeInterval, spaced: Duration)
    extends EtlTask[Monitoring, Unit] {

  override protected def process: RIO[Monitoring, Unit] = {
    val checkUndeliveredMessages: RIO[Monitoring, Long] = Monitoring
      .getMetric(project, "pubsub.googleapis.com/subscription/num_undelivered_messages", interval)
      .map { metrics =>
        metrics
          .find(_.getResource.getLabelsMap.get("subscription_id") == subscription)
          .map(_.getPointsList.get(0).getValue.getInt64Value)
          .getOrElse(-1)
      }

    val program: RIO[Monitoring, Unit] = for {
      count <- checkUndeliveredMessages
      _ <-
        if (count == 0) ZIO.logInfo(s"$project:$subscription count is 0")
        else if (count == -1) ZIO.fail(RetryException(s"$project:$subscription hasn't appeared yet"))
        else ZIO.fail(RetryException(s"$project:$subscription count is $count"))
    } yield ()

    val runnable: RIO[Monitoring, Unit] = for {
      _ <- ZIO.logInfo(s"Starting cloud monitoring sensor for num_undelivered_messages on subscription $project:$subscription")
      _ <- program.retry(RetrySchedule.forever(spaced))
    } yield ()

    runnable
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getMetaData: Map[String, String] =
    Map("project" -> project, "subscription" -> subscription, "interval" -> interval.toString, "spaced" -> spaced.toString)
}
