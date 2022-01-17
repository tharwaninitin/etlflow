package etlflow.log

import etlflow.utils.{ApplicationLogger, DateTimeApi}
import zio.{Ref, UIO, ULayer, ZLayer}
import scala.collection.mutable

object Memory extends ApplicationLogger {

  sealed trait Status
  object Status {
    case object Running                 extends Status
    case object Succeed                 extends Status
    case class Failed(error: Throwable) extends Status
  }
  case class State(step_name: String, status: Status, start_time: Long, end_time: Option[Long]) {
    override def toString: String =
      s"$step_name,$status,${DateTimeApi.getTimestampAsString(start_time)},${DateTimeApi.getTimestampAsString(end_time.getOrElse(0L))}"
  }

  val state: UIO[Ref[mutable.Map[String, State]]] = Ref.make(mutable.Map.empty[String, State])
  // format: off
  case class MemoryLogger(job_run_id: String) extends Service {
    override def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): UIO[Unit] =
      for {
        stateRef <- state
        _     <- stateRef.update{ st =>
                    st.update(step_run_id,State(step_name, Status.Running, DateTimeApi.getCurrentTimestamp, None))
                    st
                  }
      } yield ()
    override def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable]): UIO[Unit] =
      for {
        stateRef <- state
        _     <- stateRef.update{ st =>
                  if(error.isEmpty) {
                    st.update(step_run_id, st(step_run_id).copy(status = Status.Succeed, end_time = Some(DateTimeApi.getCurrentTimestamp)))
                  } else
                    st.update(step_run_id, st(step_run_id).copy(status = Status.Failed(error.get), end_time = Some(DateTimeApi.getCurrentTimestamp)))
                  st
                }
      } yield ()
    override def logJobStart(job_name: String, args: String, start_time: Long): UIO[Unit] =
      UIO(logger.info(s"Job $job_name started"))
    override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): UIO[Unit] = {
      for {
        stateRef <- state
        value    <- stateRef.get
        _        = if(error.isEmpty) {
                      logger.info(s"Job completed with success")
                      value.values.toList.sortBy(_.start_time).foreach(x => logger.info(x.toString()))
                    } else {
                      logger.error(s"Job completed with failure ${error.get.getMessage}")
                      value.values.toList.sortBy(_.start_time).foreach(x => logger.info(x.toString()))
                    }
      } yield ()
    }
  }
  // format: on

  def live(job_run_id: String): ULayer[LogEnv] = ZLayer.succeed(MemoryLogger(job_run_id))
}
