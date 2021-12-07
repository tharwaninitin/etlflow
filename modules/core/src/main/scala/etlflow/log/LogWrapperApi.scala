package etlflow.log

import etlflow.etlsteps.EtlStep
import zio.{RIO, UIO, URIO, ZIO}

object LogWrapperApi {

  trait Service {
    def setJobRunId(jri: String): UIO[Unit]
    def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[DBLogEnv with ConsoleLogEnv, Unit]
    def jobLogEnd(start_time: Long, job_run_id: String, job_name: String, ex: Option[Throwable]): RIO[DBLogEnv with ConsoleLogEnv with SlackLogEnv, Unit]
    def stepLogStart(start_time: Long, etlStep: EtlStep[_,_]): RIO[DBLogEnv with ConsoleLogEnv, Unit]
    def stepLogEnd(start_time: Long, etlStep: EtlStep[_,_], ex: Option[Throwable]): RIO[DBLogEnv with ConsoleLogEnv with SlackLogEnv, Unit]
  }

  def setJobRunId(jri: String): URIO[LogWrapperEnv, Unit] = ZIO.accessM[LogWrapperEnv](_.get.setJobRunId(jri))
  def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[LogWrapperEnv with DBLogEnv with ConsoleLogEnv, Unit] =
    ZIO.accessM[LogWrapperEnv with DBLogEnv with ConsoleLogEnv](_.get.jobLogStart(start_time,job_type,job_name,props,is_master))
  def jobLogEnd(start_time: Long, job_run_id: String, job_name: String, ex: Option[Throwable] = None): RIO[LogWrapperEnv with DBLogEnv with ConsoleLogEnv with SlackLogEnv, Unit] =
    ZIO.accessM[LogWrapperEnv with DBLogEnv with ConsoleLogEnv with SlackLogEnv](_.get.jobLogEnd(start_time,job_run_id,job_name,ex))
  def stepLogStart(start_time: Long, etlStep: EtlStep[_,_]): RIO[LogWrapperEnv with DBLogEnv with ConsoleLogEnv, Unit] =
    ZIO.accessM[LogWrapperEnv with DBLogEnv with ConsoleLogEnv](_.get.stepLogStart(start_time, etlStep))
  def stepLogEnd(start_time: Long, etlStep: EtlStep[_,_], ex: Option[Throwable] = None): RIO[LogWrapperEnv with DBLogEnv with ConsoleLogEnv with SlackLogEnv, Unit] =
    ZIO.accessM[LogWrapperEnv with DBLogEnv with ConsoleLogEnv with SlackLogEnv](_.get.stepLogEnd(start_time, etlStep, ex))
}
