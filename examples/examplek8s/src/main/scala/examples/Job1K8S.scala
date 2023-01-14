package examples

import etlflow.audit
import etlflow.audit.Audit
import etlflow.k8s._
import etlflow.log.ApplicationLogger
import etlflow.task._
import zio._
import java.util.UUID

object Job1K8S extends ZIOAppDefault with ApplicationLogger {

  override val bootstrap: ULayer[Unit] = zioSlf4jLogger

  private val jobName = s"hello-${UUID.randomUUID.toString.take(8)}"

  val program: RIO[K8S with Audit, Unit] = for {
    _ <- CreateKubeJobTask(
      name = "CreateKubeJobTask",
      jobName = jobName,
      container = jobName,
      image = "busybox:1.28",
      command = List("/bin/sh", "-c", "sleep 5; ls /etc/key; date; echo Hello from the Kubernetes cluster")
      // volumeMounts = List("secrets" -> "/etc/key")
    ).execute
    _ <- TrackKubeJobTask("TrackKubeJobTask", jobName).execute
    _ <- GetKubeJobLogTask("GetKubeJobLogTask", jobName).execute
    _ <- DeleteKubeJobTask("DeleteKubeJobTask", jobName).execute
  } yield ()

  override def run: Task[Unit] = ZIO.logInfo("Starting Job1K8S") *> program.provide(K8S.live() ++ audit.noop)
}
