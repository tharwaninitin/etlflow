package etlflow.log

import etlflow.db.DBEnv
import etlflow.etlsteps.EtlStep
import etlflow.json.JsonEnv
import zio.{RIO, UIO, URIO, ZIO}

object LoggerApi {

  trait Service {
    def setJobRunId(jri: String): UIO[Unit]
    def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[DBEnv with ConsoleEnv, Unit]
    def jobLogEnd(start_time: Long, job_run_id: String, job_name: String, ex: Option[Throwable]): RIO[DBEnv with ConsoleEnv with SlackEnv, Unit]
    def stepLogStart(start_time: Long, etlStep: EtlStep[_,_]): RIO[DBEnv with ConsoleEnv with JsonEnv, Unit]
    def stepLogEnd(start_time: Long, etlStep: EtlStep[_,_], ex: Option[Throwable]): RIO[DBEnv with ConsoleEnv with SlackEnv with JsonEnv, Unit]
  }

  def setJobRunId(jri: String): URIO[LoggerEnv, Unit] = ZIO.accessM[LoggerEnv](_.get.setJobRunId(jri))
  def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[LoggerEnv with DBEnv with ConsoleEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with ConsoleEnv](_.get.jobLogStart(start_time,job_type,job_name,props,is_master))
  def jobLogEnd(start_time: Long, job_run_id: String, job_name: String, ex: Option[Throwable] = None): RIO[LoggerEnv with DBEnv with ConsoleEnv with SlackEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with ConsoleEnv with SlackEnv](_.get.jobLogEnd(start_time,job_run_id,job_name,ex))
  def stepLogStart(start_time: Long, etlStep: EtlStep[_,_]): RIO[LoggerEnv with DBEnv with ConsoleEnv with JsonEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with ConsoleEnv with JsonEnv](_.get.stepLogStart(start_time, etlStep))
  def stepLogEnd(start_time: Long, etlStep: EtlStep[_,_], ex: Option[Throwable] = None): RIO[LoggerEnv with DBEnv with ConsoleEnv with SlackEnv with JsonEnv, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with ConsoleEnv with SlackEnv with JsonEnv](_.get.stepLogEnd(start_time, etlStep, ex))
}
