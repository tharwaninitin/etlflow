package etlflow.log

import etlflow.utils.{ApplicationLogger, DateTimeApi}
import zio.{Ref, Task, UIO, ULayer, ZLayer}

object Memory extends ApplicationLogger {

  sealed trait Status
  object Status {
    case object Running  extends Status
    case object Succeed extends Status
    case class Failed(error: Throwable)  extends Status
  }
  case class State(step_name: String, status: Status, start_time: Long, end_time: Option[Long])

  val state: UIO[Ref[Map[String,State]]] = Ref.make(Map.empty[String,State])

  case class MemoryLogger(job_run_id: String) extends Service{
    override def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): Task[Unit] =
      for {
        stateRef <- state
        _     <- stateRef.update{ st =>
                    st + (step_run_id -> State(step_name, Status.Running, DateTimeApi.getCurrentTimestamp, None))
                  }
      } yield ()
    override def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable]): Task[Unit] =
      for {
        stateRef <- state
        _     <- stateRef.update{ st =>
                  val new_step_state = if(error.isEmpty)
                      State(step_name, Status.Succeed, DateTimeApi.getCurrentTimestamp, Some(DateTimeApi.getCurrentTimestamp))
                    else
                      State(step_name, Status.Failed(error.get), DateTimeApi.getCurrentTimestamp, Some(DateTimeApi.getCurrentTimestamp))
                  val new_state = st - step_run_id
                  new_state + (step_run_id -> new_step_state)
                }
      } yield ()
    override def logJobStart(job_name: String, args: String, start_time: Long): Task[Unit] =
      UIO(logger.info(s"Job  started"))
    override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): Task[Unit] = {
      for {
        stateRef <- state
        value    <- stateRef.get
        _        = if(error.isEmpty) {
                      logger.info(s"Job completed with success")
                      logger.info(value.toString())
                    } else {
                      logger.error(s"Job completed with failure ${error.get.getMessage}")
                      logger.error(value.toString())
                    }
      } yield ()
    }
  }

  def live(job_run_id: String): ULayer[LogEnv] = ZLayer.succeed(MemoryLogger(job_run_id))
}