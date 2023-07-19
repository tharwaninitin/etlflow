package etlflow.audit

import zio.{UIO, ZIO}

object Console extends Audit {

  override val jobRunId: String = ""

  override def logTaskStart(
      taskRunId: String,
      taskName: String,
      metadata: String,
      taskType: String
  ): UIO[Unit] = ZIO.logInfo(s"Task $taskName started")

  override def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit] = error.fold {
    ZIO.logInfo(s"Task $taskRunId completed successfully")
  } { ex =>
    ZIO.logError(s"Task failed, Error StackTrace:" + "\n" + ex.getStackTrace.mkString("\n"))
  }

  override def logJobStart(jobName: String, metadata: String): UIO[Unit] =
    ZIO.logInfo(s"Job $jobName started")

  override def logJobEnd(error: Option[Throwable]): UIO[Unit] = error.fold {
    ZIO.logInfo(s"Job completed with success")
  } { ex =>
    ZIO.logError(s"Job completed with failure ${ex.getMessage}")
  }
}
