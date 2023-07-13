package etlflow.audit

import etlflow.model._
import zio.{RIO, Task, UIO, URIO, ZIO}

/** The `Audit` interface provides methods for logging and querying the execution status of tasks and jobs (collections of tasks).
  * The auditing interface is integrated with the [[etlflow.task.EtlTask]] Interface. Each task uses this interface to maintain
  * the state of all tasks in the job/workflow in the backend of choice for end-to-end auditability
  */
@SuppressWarnings(Array("org.wartremover.warts.ToString"))
trait Audit {
  val jobRunId: String

  /** Get the backend name.
    *
    * @return
    *   The backend name.
    */
  def backend: UIO[String] = ZIO.succeed(this.getClass.getSimpleName)

  /** Log the start of a job.
    *
    * @param jobName
    *   The name of the job.
    * @param props
    *   The properties associated with the job.
    */
  def logJobStart(jobName: String, props: String): UIO[Unit]

  /** Log the end of a job, with an optional error.
    *
    * @param error
    *   An optional error that occurred during the job execution.
    */
  def logJobEnd(error: Option[Throwable]): UIO[Unit]

  /** Log the start of a task.
    *
    * @param taskRunId
    *   The ID of the task run.
    * @param taskName
    *   The name of the task.
    * @param props
    *   The properties associated with the task.
    * @param taskType
    *   The type of the task.
    */
  def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): UIO[Unit]

  /** Log the end of a task, with an optional error.
    *
    * @param taskRunId
    *   The ID of the task run.
    * @param error
    *   An optional error that occurred during the task execution.
    */
  def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit]

  /** Get the list of job runs based on a query.
    *
    * @param query
    *   The query to fetch job runs.
    * @return
    *   The list of job runs.
    */
  def getJobRuns(query: String): Task[Iterable[JobRun]] = ZIO.logInfo(query) *> ZIO.succeed(List.empty[JobRun])

  /** Get the list of task runs based on a query.
    *
    * @param query
    *   The query to fetch task runs.
    * @return
    *   The list of task runs.
    */
  def getTaskRuns(query: String): Task[Iterable[TaskRun]] = ZIO.logInfo(query) *> ZIO.succeed(List.empty[TaskRun])

  /** Fetch results from a query and transform them using the provided function.
    *
    * @param query
    *   The query to fetch results.
    * @param fn
    *   The function to transform the results.
    * @tparam RS
    *   The result type of the query.
    * @tparam T
    *   The transformed result type.
    * @return
    *   The transformed results.
    */
  def fetchResults[RS, T](query: String)(fn: RS => T): Task[Iterable[T]] =
    ZIO.logInfo(query + fn.toString) *> ZIO.succeed(Iterable.empty)

  /** Execute a query without returning any results.
    *
    * @param query
    *   The query to execute.
    */
  def executeQuery(query: String): Task[Unit] = ZIO.logInfo(query)
}

object Audit {

  /** Get the backend name.
    *
    * @return
    *   The backend name.
    */
  def backend: RIO[Audit, String] = ZIO.serviceWithZIO(_.backend)

  /** Log the start of a job.
    *
    * @param jobName
    *   The name of the job.
    * @param props
    *   The properties associated with the job.
    */
  def logJobStart(jobName: String, props: String): URIO[Audit, Unit] = ZIO.serviceWithZIO(_.logJobStart(jobName, props))

  /** Log the end of a job, with an optional error.
    *
    * @param error
    *   An optional error that occurred during the job execution.
    */
  def logJobEnd(error: Option[Throwable] = None): URIO[Audit, Unit] = ZIO.serviceWithZIO(_.logJobEnd(error))

  /** Log the start of a task.
    *
    * @param taskRunId
    *   The ID of the task run.
    * @param taskName
    *   The name of the task.
    * @param props
    *   The properties associated with the task.
    * @param taskType
    *   The type of the task.
    */
  def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): URIO[Audit, Unit] =
    ZIO.serviceWithZIO(_.logTaskStart(taskRunId, taskName, props, taskType))

  /** Log the end of a task, with an optional error.
    *
    * @param taskRunId
    *   The ID of the task run.
    * @param error
    *   An optional error that occurred during the task execution.
    */
  def logTaskEnd(taskRunId: String, error: Option[Throwable] = None): URIO[Audit, Unit] =
    ZIO.serviceWithZIO(_.logTaskEnd(taskRunId, error))

  /** Get the list of job runs based on a query.
    *
    * @param query
    *   The query to fetch job runs.
    * @return
    *   The list of job runs.
    */
  def getJobRuns(query: String): RIO[Audit, Iterable[JobRun]] = ZIO.serviceWithZIO(_.getJobRuns(query))

  /** Get the list of task runs based on a query.
    *
    * @param query
    *   The query to fetch task runs.
    * @return
    *   The list of task runs.
    */
  def getTaskRuns(query: String): RIO[Audit, Iterable[TaskRun]] = ZIO.serviceWithZIO(_.getTaskRuns(query))

  /** Fetch results from a query and transform them using the provided function.
    *
    * @param query
    *   The query to fetch results.
    * @param fn
    *   The function to transform the results.
    * @tparam RS
    *   The result type of the query.
    * @tparam T
    *   The transformed result type.
    * @return
    *   The transformed results.
    */
  def fetchResults[RS, T](query: String)(fn: RS => T): RIO[Audit, Iterable[T]] =
    ZIO.serviceWithZIO[Audit](_.fetchResults(query)(fn))

  /** Execute a query without returning any results.
    *
    * @param query
    *   The query to execute.
    */
  def executeQuery(query: String): RIO[Audit, Unit] = ZIO.serviceWithZIO(_.executeQuery(query))
}
