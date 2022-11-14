package etlflow.audit

import zio.{UIO, ZIO}

object Console extends Audit {

  override val jobRunId: String = ""

  override def logTaskStart(
      taskRunId: String,
      taskName: String,
      props: Map[String, String],
      taskType: String,
      startTime: Long
  ): UIO[Unit] = ZIO.logInfo(s"Task $taskName started")

  override def logTaskEnd(
      taskRunId: String,
      taskName: String,
      props: Map[String, String],
      taskType: String,
      endTime: Long,
      error: Option[Throwable]
  ): UIO[Unit] = error.fold {
    ZIO.logInfo(s"Task $taskName completed successfully")
  } { ex =>
    ZIO.logError(s"Task $taskName failed, Error StackTrace:" + "\n" + ex.getStackTrace.mkString("\n"))
  }

  override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] = ZIO.logInfo(s"Job started")

  override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] = error.fold {
    ZIO.logInfo(s"Job completed with success")
  } { ex =>
    ZIO.logError(s"Job completed with failure ${ex.getMessage}")
  }
}
