package etlflow

import cron4s.CronExpr
import doobie.hikari.HikariTransactor
import etlflow.schema.Credential.JDBC
import etlflow.utils.DbManager
import zio.blocking.Blocking
import zio.{Has, Task, ZLayer}

package object jdbc extends DbManager {

  type TransactorEnv = Has[HikariTransactor[Task]]
  type DBEnv = Has[DB.Service]
  case class DBException(msg : String) extends RuntimeException(msg)

  case class DbStepRunArgs(job_run_id: String)
  case class DbJobRunArgs(
                           jobRunId: Option[String] = None, jobName: Option[String] = None, startTime: Option[java.time.LocalDate] = None,
                           endTime: Option[java.time.LocalDate] = None, filter: Option[String] = None, limit: Long, offset: Long
                         )

  case class JobLogsArgs(filter: Option[Double] = None, limit:Option[Long] = None)
  case class GetCredential(name: String, `type`: String, valid_from: String)
  case class JobLogs(job_name: String,  success: String, failed: String)
  case class EtlJobStateArgs(name: String, state: Boolean)
  case class Credentials(name: String, `type`: String, value: String)
  case class JobRun(job_run_id: String,job_name: String,properties: String,state: String,start_time: String,elapsed_time: String,job_type: String,is_master:String)
  case class StepRun(job_run_id: String,step_name: String,properties: String,state: String,start_time: String,elapsed_time:String,step_type:String,step_run_id:String)
  case class Job(
                  name: String, props: Map[String,String], schedule: Option[CronExpr], nextSchedule: String, schduleRemainingTime: String ,
                  failed: Long, success: Long, is_active:Boolean, last_run_time: Long, last_run_description: String
                )
  case class EtlJob(name: String, props: Map[String,String])
  case class JsonString(str: String) extends AnyVal
  case class CredentialDB(name: String, `type`: String, value: JsonString)

  case class UserDB(user_name: String, password: String, user_active: String, user_role: String)
  case class JobDB(job_name: String, schedule: String, is_active: Boolean)
  case class JobDBAll(job_name: String, job_description: String, schedule: String, failed: Long, success: Long, is_active: Boolean, last_run_time: Option[Long] = None)

  case class JobRunDB(job_run_id: String,job_name: String,properties: String,state: String,elapsed_time: String,job_type: String,is_master:String,inserted_at:Long)
  case class StepRunDB(job_run_id: String,step_name: String,properties: String,state: String,elapsed_time:String,step_type:String,step_run_id:String, inserted_at:Long)

  def liveDBWithTransactor(db: JDBC): ZLayer[Blocking, Throwable, TransactorEnv with DBEnv] = liveTransactor(db: JDBC) >+> DB.liveDB
}
