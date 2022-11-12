package examples

import etlflow.k8s.K8S
import etlflow.log.ApplicationLogger
import etlflow.task._
import zio._

object Job5K8S extends ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  val jobName = "hello"

  private val program = for {
    _ <- DeleteKubeJobTask(jobName).execute.ignore
    job <- CreateKubeJobTask(
      name = jobName,
      image = "busybox:1.28",
      command = Some(Vector("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster"))
    ).execute
    _ <- TrackKubeJobTask(jobName).execute
  } yield ()

  override def run: Task[Unit] = program.provide(K8S.live ++ etlflow.audit.test)
}
