package examples

import etlflow.audit
import etlflow.audit.Audit
import etlflow.k8s._
import etlflow.log.ApplicationLogger
import etlflow.task._
import zio._

object Job1K8S extends ZIOAppDefault with ApplicationLogger {

  override val bootstrap: ULayer[Unit] = zioSlf4jLogger

  private val jobName = s"hello-busybox"

  val program: RIO[K8S with Audit, Unit] = for {
    _ <- K8SJobTask(
      name = "CreateKubeJobTask",
      jobName = jobName,
      image = "busybox:1.28",
      command = Some(List("/bin/sh", "-c", "sleep 5; ls /etc/key; date; echo Hello from the Kubernetes cluster"))
      // volumeMounts = Some(List("/etc/key" -> "secrets"))
    ).toZIO
    _ <- K8STrackJobTask("TrackKubeJobTask", jobName).toZIO
    _ <- K8SJobLogTask("GetKubeJobLogTask", jobName).toZIO
    _ <- K8SDeleteJobTask("DeleteKubeJobTask", jobName).toZIO
  } yield ()

  override def run: Task[Unit] = ZIO.logInfo("Starting Job1K8S") *> program.provide(K8S.live() ++ audit.noop)
}
