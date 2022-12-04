package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.model.K8sNamespace
import etlflow.k8s.K8S.getJob
import etlflow.k8s._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.{RIO, ZIO}
import scala.concurrent.duration._

/** Track kubernetes job and waits for successful execution of pod for given job name
  * @param name
  *   kubernetes job name
  * @param namespace
  *   kubernetes cluster namespace defaults to default namespace
  * @param retry
  *   Number of times we need to check for status of kube job before this effect terminates
  * @param spaced
  *   Specifies duration each repetition should be spaced from the last run
  */
case class TrackKubeJobTask(
    name: String,
    namespace: K8sNamespace = K8sNamespace.default,
    retry: Option[Int] = None,
    spaced: Duration = 5.seconds
) extends EtlTask[K8S with Jobs, Unit] {

  override protected def process: RIO[K8S with Jobs, Unit] = {
    val program: RIO[K8S with Jobs, Unit] =
      for {
        job <- getJob(name)
        podSucceeded = job.status.flatMap(_.succeeded).getOrElse(-1)
        _ <-
          if (podSucceeded == 1) ZIO.logInfo(s"Job Completed, Pods Succeeded $podSucceeded")
          else
            ZIO.fail(RetryException(s"Pods Succeeded $podSucceeded"))
      } yield ()

    val runnable: RIO[K8S with Jobs, Unit] = for {
      _ <- ZIO.logInfo("#" * 50)
      _ <- ZIO.logInfo("Started Polling Job Status")
      _ <- retry.map(r => program.retry(RetrySchedule.recurs(r, spaced))).getOrElse(program.retry(RetrySchedule.forever(spaced)))
      _ <- ZIO.logInfo("#" * 50)
    } yield ()

    runnable
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
