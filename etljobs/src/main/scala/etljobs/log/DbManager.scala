package etljobs.log

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import etljobs.{EtlJobName, EtlProps}
import etljobs.etlsteps.EtlStep
import io.getquill.{LowerCase, PostgresJdbcContext}
import scala.util.{Failure, Success, Try}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

object DbManager extends LogManager {
  case class JobRun(
                     job_run_id: String,
                     job_name: String,
                     description: String,
                     properties: String,
                     state: String,
                     inserted_at: Long
                   )
  case class StepRun(
                      job_run_id: String,
                      step_name: String,
                      properties: String,
                      state: String,
                      elapsed_time:String
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
      error_message: Option[String] = None,
      mode: String = "update"
    ): Unit = {
    ctx match {
      case Success(context) =>
        import context._
        if (mode == "insert") {
          val step = StepRun(
            job_run_id,
            etl_step.name,
            etl_step.getStepProperties(notification_level).mkString(", "),
            state_status.toLowerCase(),
            "..."
          )
          lm_logger.info(step)
          val s = quote {
            querySchema[StepRun]("step").insert(lift(step))
          }
          context.run(s)
        }
        else if (mode == "update") {
          val execution_end_time = System.nanoTime()
          val status = if (error_message.isDefined) state_status + " with error: " + error_message.get else state_status
          val elapsed_time = (math floor ((execution_end_time - execution_start_time) / 1000000000.0)) + " secs"
          lm_logger.info(s"Updating step info in db with status => $status")
          context.run( quote {
            querySchema[StepRun]("step")
              .filter(x => x.job_run_id == lift(job_run_id) && x.step_name == lift(etl_step.name))
              .update(
                _.state -> lift(status),
                _.elapsed_time -> lift(elapsed_time)
                )
          })
        }

      case Failure(e) => lm_logger.error(s"Cannot log step properties to log db => ${e.getMessage}")
    }
  }

  override def updateJobInformation(status: String, etlProps: Option[EtlProps] = None, mode: String = "update"): Unit = {
    ctx match {
      case Success(context) =>
        import context._
        if (mode == "insert") {
          implicit val formats = DefaultFormats
          val propsJson = write(etlProps.get)
          val job = JobRun(job_run_id, job_name.toString, "This is job description", propsJson, "started", System.currentTimeMillis())
          lm_logger.info(job)
          context.run( quote {
            query[JobRun].insert(lift(job))
          })
        }
        else if (mode == "update") {
          lm_logger.info(s"Updating job info in db with status => $status")
          context.run( quote {
            query[JobRun].filter(_.job_run_id == lift(job_run_id)).update(_.state -> lift(status))
          })
        }

      case Failure(e) => lm_logger.error(s"Cannot update job state to log db => ${e.getMessage}")
    }
  }
}
