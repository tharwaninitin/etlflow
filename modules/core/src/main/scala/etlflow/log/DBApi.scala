package etlflow.log

import zio.{Task, ZIO}

object DBApi {
  trait Service {
    def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): Task[Unit]
    def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): Task[Unit]
    def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): Task[Unit]
    def updateJobRun(job_run_id: String, status: String, elapsed_time: String): Task[Unit]
  }
  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): ZIO[DBLogEnv, Throwable, Unit] =
    ZIO.accessM(_.get.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time))
  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): ZIO[DBLogEnv, Throwable, Unit] =
    ZIO.accessM(_.get.updateStepRun(job_run_id, step_name, props, status, elapsed_time))
  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): ZIO[DBLogEnv, Throwable, Unit] =
    ZIO.accessM(_.get.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time))
  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): ZIO[DBLogEnv, Throwable, Unit] =
    ZIO.accessM(_.get.updateJobRun(job_run_id, status, elapsed_time))

}
