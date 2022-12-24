package etlflow.audit

import etlflow.model._
import zio.{UIO, URIO, ZIO}

// format: off
trait Audit {
  val jobRunId: String

  def logJobStart(jobName: String, args: Map[String,String], props: Map[String,String]): UIO[Unit]
  def logJobEnd(jobName: String, args: Map[String,String], props: Map[String,String], error: Option[Throwable]): UIO[Unit]

  def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String): UIO[Unit]
  def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, error: Option[Throwable]): UIO[Unit]
  
  def getJobRuns(query: String): UIO[Iterable[JobRun]]
  def getTaskRuns(query: String): UIO[Iterable[TaskRun]]
}

object Audit {
  def logJobStart(jobName: String, args: Map[String,String], props: Map[String,String]): URIO[Audit, Unit] =
    ZIO.environmentWithZIO(_.get.logJobStart(jobName, args, props))
  def logJobEnd(jobName: String, args: Map[String,String], props: Map[String,String], error: Option[Throwable] = None): URIO[Audit, Unit] =
    ZIO.environmentWithZIO(_.get.logJobEnd(jobName, args, props, error))

  def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String): URIO[Audit, Unit] =
    ZIO.environmentWithZIO(_.get.logTaskStart(taskRunId, taskName, props, taskType))
  def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, error: Option[Throwable] = None): URIO[Audit, Unit] =
    ZIO.environmentWithZIO(_.get.logTaskEnd(taskRunId, taskName, props, taskType, error))

  def getJobRuns(query: String): URIO[Audit ,Iterable[JobRun]] = ZIO.environmentWithZIO(_.get.getJobRuns(query))
  def getTaskRuns(query: String): URIO[Audit, Iterable[TaskRun]] = ZIO.environmentWithZIO(_.get.getTaskRuns(query))
}
// format: on
