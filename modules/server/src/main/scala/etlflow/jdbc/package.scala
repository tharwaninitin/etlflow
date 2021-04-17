package etlflow

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import etlflow.Credential.JDBC
import etlflow.log.ApplicationLogger
import zio.blocking.Blocking
import zio.{Has, Task, ZIO, ZLayer}

package object jdbc extends DbManager with ApplicationLogger {
  case class UserInfo(user_name: String, password: String, user_active: String,user_role:String)
  case class JobDB(job_name: String, schedule: String, is_active: Boolean)
  case class JobDBAll(job_name: String, job_description: String, schedule: String, failed: Long, success: Long, is_active: Boolean, last_run_time: Option[Long] = None)
  case class JsonString(str: String) extends AnyVal
  case class CredentialDB(name: String, `type`: String, value: JsonString)

  type TransactorEnv = Has[HikariTransactor[Task]]
  type DBEnv = Has[DB.Service]

  def liveTransactor(db: JDBC, pool_name: String = "EtlFlow-Pool", pool_size: Int = 10): ZLayer[Blocking, Throwable, TransactorEnv] = ZLayer.fromManaged(
      for {
        rt         <- Task.runtime.toManaged_
        blocker    <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
        transactor <- createDbTransactorManaged(db, rt.platform.executor.asEC, pool_name, pool_size)(blocker)
      } yield transactor
    )
  def liveDBWithTransactor(db: JDBC): ZLayer[Blocking, Throwable, DBEnv] = liveTransactor(db: JDBC) >>> DB.liveDB
}
