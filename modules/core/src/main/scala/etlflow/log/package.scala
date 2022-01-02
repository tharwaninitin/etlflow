package etlflow

import zio.{Has, Task, ULayer, ZIO, ZLayer}

package object log {
  type LogEnv = Has[Service]

  trait Service {
    val job_run_id: String
    def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): Task[Unit]
    def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable]): Task[Unit]
    def logJobStart(job_name: String, args: String, start_time: Long): Task[Unit]
    def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): Task[Unit]
  }

  object LogApi {
    def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): ZIO[LogEnv, Throwable, Unit] =
      ZIO.accessM(_.get.logStepStart(step_run_id, step_name, props, step_type, start_time))
    def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable] = None): ZIO[LogEnv, Throwable, Unit] =
      ZIO.accessM(_.get.logStepEnd(step_run_id, step_name, props, step_type, end_time, error))
    def logJobStart(job_name: String, args: String, start_time: Long): ZIO[LogEnv, Throwable, Unit] =
      ZIO.accessM(_.get.logJobStart(job_name, args, start_time))
    def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable] = None): ZIO[LogEnv, Throwable, Unit] =
      ZIO.accessM(_.get.logJobEnd(job_name, args, end_time, error))
  }

  val nolog: ULayer[LogEnv] = ZLayer.succeed(
    new Service {
      override val job_run_id: String = ""
      override def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): Task[Unit] = ZIO.unit
      override def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable]): Task[Unit] = ZIO.unit
      override def logJobStart(job_name: String, args: String, start_time: Long): Task[Unit] = ZIO.unit
      override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): Task[Unit] = ZIO.unit
    }
  )
}
