package examples

import etlflow.k8s.K8S
import etlflow.log.ApplicationLogger
import etlflow.task._
import zio._

object Job1K8S extends ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  val jobName = "hello"

  private val program = for {
    _ <- DeleteKubeJobTask(jobName).execute.ignore
    _ <- CreateKubeJobTask(
      name = jobName,
      image = "busybox:1.28",
      command = Some(Vector("/bin/sh", "-c", "sleep 5; date; echo Hello from the Kubernetes cluster"))
    ).execute
    _ <- TrackKubeJobTask(jobName).execute
  } yield ()

  override def run: Task[Unit] = program.provide(K8S.live(logRequestResponse = true) ++ etlflow.audit.test)
}
