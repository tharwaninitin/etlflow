package etlflow

import etlflow.Credential.JDBC
import etlflow.log.ApplicationLogger
import zio.blocking.Blocking
import zio.{Has, ZLayer}

package object jdbc extends DbManager with ApplicationLogger {
  case class UserDB(user_name: String, password: String, user_active: String, user_role: String)
  case class JobDB(job_name: String, schedule: String, is_active: Boolean)
  case class JobDBAll(job_name: String, job_description: String, schedule: String, failed: Long, success: Long, is_active: Boolean, last_run_time: Option[Long] = None)
  case class JsonString(str: String) extends AnyVal
  case class CredentialDB(name: String, `type`: String, value: JsonString)

  type DBEnv = Has[DB.Service]
  def liveDBWithTransactor(db: JDBC): ZLayer[Blocking, Throwable, TransactorEnv with DBEnv] = liveTransactor(db: JDBC) >+> DB.liveDB
}
