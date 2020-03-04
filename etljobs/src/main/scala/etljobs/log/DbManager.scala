package etljobs.log

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import etljobs.EtlJobName
import etljobs.etlsteps.EtlStep
import io.getquill.{LowerCase, PostgresJdbcContext}
import scala.util.{Failure, Success, Try}

object DbManager extends LogManager {
  // CREATE TABLE jobrun(job_id int, job_run_id varchar,job_name varchar, description varchar, properties varchar, state varchar, elapsed_time varchar);
  case class JobRun(
                     job_id: Int,
                     job_run_id: String,
                     job_name: String,
                     description: Option[String] = None,
                     properties: Option[String] = None,
                     state: String,
                     elapsed_time: String
                   )

  // CREATE TABLE steprun(job_run_id varchar, step_name varchar, properties varchar, state varchar, elapsed_time varchar);
  case class StepRun(
                      job_run_id: String,
                      step_name: String,
                      properties: String,
                      state: String,
                      elapsed_time: String
                    )

  override var job_name: EtlJobName = _
  override var job_run_id: String = _
  var log_db_url: String = ""
  var log_db_user: String = ""
  var log_db_pwd: String = ""

  private lazy val ctx = Try {
    val pgDataSource = new org.postgresql.ds.PGSimpleDataSource()
    pgDataSource.setURL(log_db_url)
    pgDataSource.setUser(log_db_user)
    pgDataSource.setPassword(log_db_user)
    val config = new HikariConfig()
    config.setDataSource(pgDataSource)
    new PostgresJdbcContext(LowerCase, new HikariDataSource(config))
  }

  def updateStepLevelInformation(
      execution_start_time: Long,
      etl_step: EtlStep[Unit, Unit],
      state_status: String,
      notification_level:String,
      error_message: Option[String] = None
    ): Unit = {
    ctx match {
      case Success(context) =>
        import context._
        val execution_end_time = System.nanoTime()
        val elapsed_time = (execution_end_time - execution_start_time) / 1000000000.0 / 60.0 + " mins"
        val step = StepRun(
          job_run_id,
          etl_step.name,
          etl_step.getStepProperties(notification_level).mkString(", "),
          state_status.toLowerCase(),
          elapsed_time
        )
        println(step)
        val s = quote {
          query[StepRun].insert(lift(step))
        }
        context.run(s)
      case Failure(e) => println(s"Cannot log step properties to log db => ${e.getMessage}")
    }
  }

  override def updateJobInformation(job_run_id: String, state: String): Unit = {
    ctx match {
      case Success(context) =>
        import context._
        val s = quote {
          query[JobRun].filter(_.job_run_id == lift(job_run_id)).update(_.state -> lift(state))
        }
        context.run(s)
      case Failure(e) => println(s"Cannot update job state to log db => ${e.getMessage}")
    }
  }
}
