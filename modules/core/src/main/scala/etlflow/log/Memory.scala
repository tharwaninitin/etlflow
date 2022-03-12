package etlflow.log

import etlflow.utils.{ApplicationLogger, DateTimeApi}
import zio.{Ref, UIO, ULayer, ZLayer}
import scala.collection.mutable

object Memory extends ApplicationLogger {

  sealed trait Status
  object Status {
    final case object Running                 extends Status
    final case object Succeed                 extends Status
    final case class Failed(error: Throwable) extends Status
  }
  final case class State(step_name: String, status: Status, start_time: Long, end_time: Option[Long]) {
    override def toString: String =
      s"$step_name,$status,${DateTimeApi.getTimestampAsString(start_time)},${DateTimeApi.getTimestampAsString(end_time.getOrElse(0L))}"
  }

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  val state: UIO[Ref[mutable.Map[String, State]]] = Ref.make(mutable.Map.empty[String, State])
  final case class MemoryLogger(jobRunId: String) extends Service {
    override def logStepStart(
        stepRunId: String,
        stepName: String,
        props: Map[String, String],
        stepType: String,
        startTime: Long
    ): UIO[Unit] =
      for {
        stateRef <- state
        _ <- stateRef.update { st =>
          st.update(stepRunId, State(stepName, Status.Running, DateTimeApi.getCurrentTimestamp, None))
          st
        }
      } yield ()
    override def logStepEnd(
        stepRunId: String,
        stepName: String,
        props: Map[String, String],
        stepType: String,
        endTime: Long,
        error: Option[Throwable]
    ): UIO[Unit] =
      for {
        stateRef <- state
        _ <- stateRef.update { st =>
          error.fold {
            st.update(stepRunId, st(stepRunId).copy(status = Status.Succeed, end_time = Some(DateTimeApi.getCurrentTimestamp)))
          } { ex =>
            st.update(stepRunId, st(stepRunId).copy(status = Status.Failed(ex), end_time = Some(DateTimeApi.getCurrentTimestamp)))
          }
          st
        }
      } yield ()
    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] =
      UIO(logger.info(s"Job $jobName started"))
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

  def live(jobRunId: String): ULayer[LogEnv] = ZLayer.succeed(MemoryLogger(jobRunId))
}
