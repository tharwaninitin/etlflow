package etlflow.log

import etlflow.db.DBEnv
import etlflow.etlsteps.EtlStep
import etlflow.json.JsonEnv
import zio.{RIO, Task, UIO, URIO, ZIO}

object LoggerApi {

  trait Service {
    def setJobRunId(jri: => String): UIO[Unit]
    def getSlackLogger: UIO[Option[SlackLogger]]
    def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[DBEnv,Unit]
    def jobLogSuccess(start_time: Long, job_run_id: String, job_name: String): RIO[DBEnv, Unit]
    def jobLogError(start_time: Long, job_run_id: String, job_name: String, ex: Throwable): RIO[DBEnv, Unit]
    def stepLogInit(start_time: Long, etlStep: EtlStep[_,_]): RIO[DBEnv with JsonEnv, Unit]
    def stepLogSuccess(start_time: Long, etlStep: EtlStep[_,_]): RIO[DBEnv with JsonEnv, Unit]
    def stepLogError(start_time: Long, etlStep: EtlStep[_,_], ex: Throwable): RIO[DBEnv with JsonEnv, Unit]
  }

  def setJobRunId(jri: => String): URIO[LoggerEnv, Unit] =
    ZIO.accessM[LoggerEnv](_.get.setJobRunId(jri))
  def getSlackLogger: URIO[LoggerEnv, Option[SlackLogger]] =
    ZIO.accessM[LoggerEnv](_.get.getSlackLogger)
  def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[LoggerEnv with DBEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv](_.get.jobLogStart(start_time,job_type,job_name,props,is_master))
  def jobLogSuccess(start_time: Long, job_run_id: String, job_name: String): RIO[LoggerEnv with DBEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv](_.get.jobLogSuccess(start_time,job_run_id,job_name))
  def jobLogError(start_time: Long, job_run_id: String, job_name: String, ex: Throwable): RIO[LoggerEnv with DBEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv](_.get.jobLogError(start_time,job_run_id,job_name,ex))
  def stepLogInit(start_time: Long, etlStep: EtlStep[_,_]): RIO[LoggerEnv with DBEnv with JsonEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.stepLogInit(start_time, etlStep))
  def stepLogSuccess(start_time: Long, etlStep: EtlStep[_,_]): RIO[LoggerEnv with DBEnv with JsonEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.stepLogSuccess(start_time, etlStep))
  def stepLogError(start_time: Long, etlStep: EtlStep[_,_], ex: Throwable): RIO[LoggerEnv with DBEnv with JsonEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.stepLogError(start_time, etlStep, ex))
}
