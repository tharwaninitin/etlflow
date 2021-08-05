package etlflow.log

import etlflow.db.DBEnv
import etlflow.etlsteps.EtlStep
import etlflow.json.JsonEnv
import zio.{RIO, Task, ZIO}

object LoggerApi {

  trait Service {
    def setJobRunId(jri: => String): Task[Unit]
    def jobLogStart(start_time: Long, job_type: String, job_name:String, props:String, is_master:String):
    RIO[LoggerEnv with DBEnv with JsonEnv,Unit]
    def jobLogEnd(start_time: Long, error_message: Option[String] = None):
    RIO[LoggerEnv with DBEnv with JsonEnv,Unit]
    def stepLogInit(start_time: Long, etlStep: EtlStep[_,_]): RIO[LoggerEnv with DBEnv with JsonEnv,Unit]
    def stepLogSuccess(start_time: Long, etlStep: EtlStep[_,_]):
    RIO[LoggerEnv with DBEnv with JsonEnv,Unit]
    def stepLogError(start_time: Long, ex: Throwable, etlStep: EtlStep[_,_]):
    RIO[LoggerEnv with DBEnv with JsonEnv,Unit]
  }

  def setJobRunId(jri: => String): ZIO[LoggerEnv, Throwable, Unit] = ZIO.accessM[LoggerEnv](_.get.setJobRunId(jri))

  def jobLogStart(start_time: Long, job_type: String, job_name:String, props:String, is_master:String ):
  ZIO[LoggerEnv with DBEnv with JsonEnv, Throwable, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.jobLogStart(start_time,job_type,job_name,props,is_master))

  def jobLogEnd(start_time: Long, error_message: Option[String] = None):
  ZIO[LoggerEnv with DBEnv with JsonEnv, Throwable, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.jobLogEnd(start_time,error_message))

  def stepLogInit(start_time: Long, etlStep: EtlStep[_,_]):
  ZIO[LoggerEnv with DBEnv with JsonEnv, Throwable, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.stepLogInit(start_time, etlStep))

  def StepLogSuccess(start_time: Long, etlStep: EtlStep[_,_]):
  ZIO[LoggerEnv with DBEnv with JsonEnv, Throwable, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.stepLogSuccess(start_time, etlStep))

  def stepLogError(start_time: Long, ex: Throwable, etlStep: EtlStep[_,_]):
  ZIO[LoggerEnv with DBEnv with JsonEnv, Throwable, Unit] =
    ZIO.accessM[LoggerEnv with DBEnv with JsonEnv](_.get.stepLogError(start_time, ex, etlStep))
}
