package etlflow

import com.zaxxer.hikari.HikariConfig
import cron4s.CronExpr
import doobie.hikari.HikariTransactor
import etlflow.schema.Credential.JDBC
import zio.blocking.Blocking
import zio.interop.catz._
import zio.interop.catz.implicits._
import zio.{Has, Task, ZLayer}

package object db {

  private[etlflow] type DBEnv = Has[DBApi.Service]

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
  case class EtlJob(name: String, props: Map[String,String])
  case class JsonString(str: String) extends AnyVal
  case class CredentialDB(name: String, `type`: String, value: JsonString)

  case class UserDB(user_name: String, password: String, user_active: String, user_role: String)
  case class JobDB(job_name: String, schedule: String, is_active: Boolean)
  case class JobDBAll(job_name: String, job_description: String, schedule: String, failed: Long, success: Long, is_active: Boolean, last_run_time: Option[Long] = None)

  case class JobRunDB(job_run_id: String,job_name: String,properties: String,state: String,elapsed_time: String,job_type: String,is_master:String,inserted_at:Long)
  case class StepRunDB(job_run_id: String,step_name: String,properties: String,state: String,elapsed_time:String,step_type:String,step_run_id:String, inserted_at:Long)

  private[db] type TransactorEnv = Has[HikariTransactor[Task]]

  private def createTransactor(db: JDBC, pool_name: String , pool_size: Int): ZLayer[Blocking, Throwable, TransactorEnv] = ZLayer.fromManaged {
      val config = new HikariConfig()
      config.setDriverClassName(db.driver)
      config.setJdbcUrl(db.url)
      config.setUsername(db.user)
      config.setPassword(db.password)
      config.setMaximumPoolSize(pool_size)
      config.setPoolName(pool_name)
      for {
        rt <- Task.runtime.toManaged_
        transactor <- HikariTransactor.fromHikariConfig[Task](config, rt.platform.executor.asEC).toManagedZIO
      } yield transactor
    }

  private[etlflow] def liveDBWithTransactor(db: JDBC, pool_name: String = "EtlFlow-Pool", pool_size: Int = 2): ZLayer[Blocking, Throwable, DBEnv] =
    createTransactor(db, pool_name, pool_size) >>> Implementation.liveDB

}
