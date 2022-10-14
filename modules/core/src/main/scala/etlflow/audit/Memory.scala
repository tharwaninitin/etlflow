package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.utils.DateTimeApi
import zio.{Ref, UIO, ULayer, ZIO, ZLayer}
import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
object Memory extends ApplicationLogger {

  sealed trait Status
  object Status {
    case object Running                       extends Status
    case object Succeed                       extends Status
    final case class Failed(error: Throwable) extends Status
  }
  final case class State(task_name: String, status: Status, start_time: Long, end_time: Option[Long]) {
    override def toString: String =
      s"$task_name,$status,${DateTimeApi.getTimestampAsString(start_time)},${DateTimeApi.getTimestampAsString(end_time.getOrElse(0L))}"
  }

  val state: UIO[Ref[mutable.Map[String, State]]] = Ref.make(mutable.Map.empty[String, State])
  final case class MemoryLogger(jobRunId: String) extends Service[UIO] {
    override def logTaskStart(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        startTime: Long
    ): UIO[Unit] =
      for {
        stateRef <- state
        _ <- stateRef.update { st =>
          st.update(taskRunId, State(taskName, Status.Running, DateTimeApi.getCurrentTimestamp, None))
          st
        }
      } yield ()
    override def logTaskEnd(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        endTime: Long,
        error: Option[Throwable]
    ): UIO[Unit] =
      for {
        stateRef <- state
        _ <- stateRef.update { st =>
          error.fold {
            st.update(taskRunId, st(taskRunId).copy(status = Status.Succeed, end_time = Some(DateTimeApi.getCurrentTimestamp)))
          } { ex =>
            st.update(taskRunId, st(taskRunId).copy(status = Status.Failed(ex), end_time = Some(DateTimeApi.getCurrentTimestamp)))
          }
          st
        }
      } yield ()
    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] =
      ZIO.succeed(logger.info(s"Job $jobName started"))
    override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      for {
        stateRef <- state
        value    <- stateRef.get
        _ = error.fold {
          logger.info(s"Job completed with success")
          value.values.toList.sortBy(_.start_time).foreach(x => logger.info(x.toString()))
        } { ex =>
          logger.error(s"Job completed with failure ${ex.getMessage}")
          value.values.toList.sortBy(_.start_time).foreach(x => logger.info(x.toString()))
        }
      } yield ()
  }

  def live(jobRunId: String): ULayer[AuditEnv] = ZLayer.succeed(MemoryLogger(jobRunId))
}
