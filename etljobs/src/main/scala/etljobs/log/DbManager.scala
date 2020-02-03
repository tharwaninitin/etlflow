package etljobs.log

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import etljobs.EtlJobName
import etljobs.etlsteps.EtlStep
import io.getquill.{LowerCase, PostgresJdbcContext}

import scala.util.{Failure, Success, Try}

object DbManager extends LogManager {
  override var job_name: EtlJobName = _
  override var job_run_id: String = _
  var log_db_name: String = ""
  var log_db_user: String = ""
  var log_db_pwd: String = ""

  private lazy val ctx = Try {
    val pgDataSource = new org.postgresql.ds.PGSimpleDataSource()
    pgDataSource.setDatabaseName(log_db_name)
    pgDataSource.setUser(log_db_user)
    val config = new HikariConfig()
    config.setDataSource(pgDataSource)
    new PostgresJdbcContext(LowerCase, new HikariDataSource(config))
  }

  def updateStepLevelInformation(
      execution_start_time: Long,
      etl_step: EtlStep[Unit,Unit],
      state_status: String,
      notification_level:String,
      error_message: Option[String] = None
    ): Unit = {
    ctx match {
      case Success(context) =>
        import context._
        val execution_end_time = System.nanoTime()
        val elapsed_time = (execution_end_time - execution_start_time) / 1000000000.0 / 60.0 + " mins"
        val step = Step(
          job_run_id,
          etl_step.name,
          etl_step.getStepProperties(notification_level).mkString(", "),
          state_status.toLowerCase(),
          elapsed_time
        )
        println(step)
        val s = quote {
          query[Step].insert(lift(
            step
          ))
        }
        context.run(s)
      case Failure(e) => println(s"Cannot log step properties to log db => ${e.getMessage}")
    }
  }
}
