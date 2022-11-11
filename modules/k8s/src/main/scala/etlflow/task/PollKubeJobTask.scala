package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.model.K8sNamespace
import etlflow.k8s.K8S.getJob
import etlflow.k8s._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.{RIO, ZIO}
import scala.concurrent.duration._

/** Get kubernetes job details and waits for success for given job name
  * @param name
  *   kubernetes job name
  * @param namespace
  *   kubernetes cluster namespace defaults to default namespace
  */
case class PollKubeJobTask(name: String, namespace: K8sNamespace = K8sNamespace.default) extends EtlTask[K8S with Jobs, Unit] {

  override protected def process: RIO[K8S with Jobs, Unit] = {
    val program: RIO[K8S with Jobs, Unit] =
      for {
        _           <- ZIO.succeed(logger.info("Polling Job"))
        jobMetadata <- getJob(name = name)
        jobStatus   <- jobMetadata.getStatus.mapError(e => new RuntimeException(s"Error: $e"))
        _ <-
          if (jobStatus.active.getOrElse(999) == jobStatus.succeeded.getOrElse(998))
            ZIO.succeed(logger.info("Job Completed"))
          else ZIO.fail(RetryException(s"Job Running ${jobMetadata.getStatus}"))
      } yield ()

    val runnable: RIO[K8S with Jobs, Unit] = for {
      _ <- ZIO.succeed(logger.info("Started Polling Job"))
      _ <- program.retry(RetrySchedule.forever(1.minute)) // To Do -> Getting error ->
    } yield ()

    runnable
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
