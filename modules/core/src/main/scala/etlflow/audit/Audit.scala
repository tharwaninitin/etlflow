package etlflow.audit

import etlflow.model._
import zio.{RIO, Task, UIO, URIO, ZIO}

// format: off
trait Audit {
  val jobRunId: String

  def logJobStart(jobName: String, props: Map[String,String]): UIO[Unit]
  def logJobEnd(jobName: String, props: Map[String,String], error: Option[Throwable]): UIO[Unit]

  def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String): UIO[Unit]
  def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, error: Option[Throwable]): UIO[Unit]
  
  def getJobRuns(query: String): Task[Iterable[JobRun]]
  def getTaskRuns(query: String): Task[Iterable[TaskRun]]
  def fetchResults[RS](query: String): Task[Iterable[RS]]
}

object Audit {
  def logJobStart(jobName: String, props: Map[String,String]): URIO[Audit, Unit] =
    ZIO.serviceWithZIO(_.logJobStart(jobName, props))
  def logJobEnd(jobName: String, props: Map[String,String], error: Option[Throwable] = None): URIO[Audit, Unit] =
    ZIO.serviceWithZIO(_.logJobEnd(jobName, props, error))

  def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String): URIO[Audit, Unit] =
    ZIO.serviceWithZIO(_.logTaskStart(taskRunId, taskName, props, taskType))
  def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, error: Option[Throwable] = None): URIO[Audit, Unit] =
    ZIO.serviceWithZIO(_.logTaskEnd(taskRunId, taskName, props, taskType, error))

  def getJobRuns(query: String): RIO[Audit ,Iterable[JobRun]] = ZIO.serviceWithZIO(_.getJobRuns(query))
  def getTaskRuns(query: String): RIO[Audit, Iterable[TaskRun]] = ZIO.serviceWithZIO(_.getTaskRuns(query))
  def fetchResults[RS](query: String): RIO[Audit, Iterable[RS]] = ZIO.serviceWithZIO(_.fetchResults[RS](query))
}
// format: on
