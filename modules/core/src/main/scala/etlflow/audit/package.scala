package etlflow

import etlflow.model.{JobRun, TaskRun}
import zio.{UIO, ULayer, ZIO, ZLayer}

package object audit {
  val noop: ULayer[Audit] = ZLayer.succeed(
    new Audit {
      override val jobRunId: String = ""

      override def logJobStart(jobName: String, props: String): UIO[Unit] = ZIO.unit

      override def logJobEnd(error: Option[Throwable]): UIO[Unit] = ZIO.unit

      override def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): UIO[Unit] = ZIO.unit

      override def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit] = ZIO.unit

      override def getJobRuns(query: String): UIO[Iterable[JobRun]] = ZIO.succeed(List.empty[JobRun])

      override def getTaskRuns(query: String): UIO[Iterable[TaskRun]] = ZIO.succeed(List.empty[TaskRun])
    }
  )

  val console: ULayer[Audit] = ZLayer.succeed(Console)

  def memory(jobRunId: String): ULayer[Audit] = ZLayer.succeed(Memory(jobRunId))

  def slack(jri: String, slackUrl: String): ULayer[Audit] = ZLayer.succeed(Slack(jri, slackUrl))
}
