package etlflow.audit

import etlflow.model._
import zio.{RIO, Task, UIO, URIO, ZIO}

// format: off
@SuppressWarnings(Array("org.wartremover.warts.ToString"))
trait Audit {
  val jobRunId: String

  def logJobStart(jobName: String, props: String): UIO[Unit]
  def logJobEnd(error: Option[Throwable]): UIO[Unit]

  def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): UIO[Unit]
  def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit]
  
  def getJobRuns(query: String): Task[Iterable[JobRun]] = ZIO.logInfo(query) *> ZIO.succeed(List.empty[JobRun])
  def getTaskRuns(query: String): Task[Iterable[TaskRun]] = ZIO.logInfo(query) *> ZIO.succeed(List.empty[TaskRun])
  
  def fetchResults[RS, T](query: String)(fn: RS => T): Task[Iterable[T]] = ZIO.logInfo(query + fn.toString) *> ZIO.succeed(Iterable.empty)
  def executeQuery(query: String): Task[Unit] = ZIO.logInfo(query) 
}

object Audit {
  def logJobStart(jobName: String, props: String): URIO[Audit, Unit] = ZIO.serviceWithZIO(_.logJobStart(jobName, props))
  def logJobEnd(error: Option[Throwable] = None): URIO[Audit, Unit] = ZIO.serviceWithZIO(_.logJobEnd(error))

  def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): URIO[Audit, Unit] = ZIO.serviceWithZIO(_.logTaskStart(taskRunId, taskName, props, taskType))
  def logTaskEnd(taskRunId: String, error: Option[Throwable] = None): URIO[Audit, Unit] = ZIO.serviceWithZIO(_.logTaskEnd(taskRunId, error))

  def getJobRuns(query: String): RIO[Audit ,Iterable[JobRun]] = ZIO.serviceWithZIO(_.getJobRuns(query))
  def getTaskRuns(query: String): RIO[Audit, Iterable[TaskRun]] = ZIO.serviceWithZIO(_.getTaskRuns(query))
  
  def fetchResults[RS, T](query: String)(fn: RS => T): RIO[Audit, Iterable[T]] = ZIO.serviceWithZIO[Audit](_.fetchResults(query)(fn))
  def executeQuery(query: String): RIO[Audit, Unit] = ZIO.serviceWithZIO(_.executeQuery(query))
}
// format: on
