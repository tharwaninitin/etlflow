package examples

import etlflow.k8s._
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
      command = Some(Vector("/bin/sh", "-c", "sleep 5; ls /etc/key; date; echo Hello from the Kubernetes cluster")),
      secret = Some(Secret("secrets", "/etc/key"))
    ).execute
    _ <- TrackKubeJobTask(jobName).execute
  } yield ()

  override def run: Task[Unit] = ZIO.logInfo("Starting Job1K8S") *> program.provide(K8S.live() ++ etlflow.audit.test)
}
