package etlflow.log

import zio.{IO, Task, ULayer, ZLayer}

object DBNoLogImplementation {
  def apply(): ULayer[DBLogEnv] = ZLayer.succeed(
    new etlflow.log.DBApi.Service {
      override def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[Throwable, Unit] = Task.unit
      override def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[Throwable, Unit] = Task.unit
      override def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[Throwable, Unit] = Task.unit
      override def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[Throwable, Unit] = Task.unit
    }
  )
}
