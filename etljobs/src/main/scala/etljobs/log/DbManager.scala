package etljobs.log

import cats.effect.{Blocker, Resource}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import etljobs.EtlJobProps
import etljobs.etlsteps.EtlStep
import etljobs.utils.{GlobalProperties, UtilityFunctions => UF}
import io.getquill.Literal
import org.apache.log4j.Logger
import zio.interop.catz._
import zio.{Task, UIO, ZIO, ZManaged}

import scala.concurrent.ExecutionContext
import scala.util.Try

class DbManager private[log](val transactor: HikariTransactor[Task],val job_name: String, val job_properties: EtlJobProps) extends LogManager[Task[Long]] {

  private val ctx = new DoobieContext.Postgres(Literal) // Literal naming scheme
  import ctx._

  def updateStepLevelInformation(
      execution_start_time: Long,
      etl_step: EtlStep[_,_],
      state_status: String,
      error_message: Option[String] = None,
      mode: String = "update"
    ): Task[Long] = {
        if (mode == "insert") {
          val step = StepRun(
            job_properties.job_run_id,
            etl_step.name,
            UF.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level)),
            state_status.toLowerCase(),
            "..."
          )
          lm_logger.info(s"Inserting step info for ${etl_step.name} in db with status => ${state_status.toLowerCase()}")
          val x: ConnectionIO[Long] = ctx.run(quote {
            querySchema[StepRun]("step").insert(lift(step))
          })
          val y: Task[Long] = x.transact(transactor)
          y
        }
        else {
          val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
          val elapsed_time = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
          lm_logger.info(s"Updating step info for ${etl_step.name} in db with status => $status")
          val x = ctx.run(quote {
            querySchema[StepRun]("step")
              .filter(x => x.job_run_id == lift(job_properties.job_run_id) && x.step_name == lift(etl_step.name))
              .update(
                _.state -> lift(status),
                _.properties -> lift(UF.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level))),
                _.elapsed_time -> lift(elapsed_time)
                )
          }).transact(transactor)
          x *> ZIO.when(error_message.isDefined)(Task.fail(new RuntimeException(error_message.get))).as(1)
        }
    }

  def updateJobInformation(status: String, mode: String = "update"): Task[Long] = {
    import ctx._
    if (mode == "insert") {
      val job = JobRun(
        job_properties.job_run_id, job_name.toString,
        job_properties.job_description,
        UF.convertToJsonByRemovingKeys(job_properties, List("job_run_id","job_description","job_properties","job_aggregate_error")),
        "started", UF.getCurrentTimestamp
      )
      lm_logger.info(s"Inserting job info in db with status => $status")
      ctx.run(quote {
        query[JobRun].insert(lift(job))
      }).transact(transactor)
    }
    else {
      lm_logger.info(s"Updating job info in db with status => $status")
      ctx.run(quote {
        query[JobRun].filter(_.job_run_id == lift(job_properties.job_run_id)).update(_.state -> lift(status))
      }).transact(transactor)
    }
  }
}

trait DbLogManager {
  val db_logger: Logger = Logger.getLogger(getClass.getName)

  def createDbResource(global_properties: Option[GlobalProperties], ec: ExecutionContext): ZManaged[Any, Throwable, HikariTransactor[Task]] = {
    val config = new HikariConfig()
    config.setDriverClassName("org.postgresql.Driver")
    config.setJdbcUrl(global_properties.map(_.log_db_url).getOrElse("<use_global_properties_log_db_url>"))
    config.setUsername(global_properties.map(_.log_db_user).getOrElse("<use_global_properties_log_db_user>"))
    config.setPassword(global_properties.map(_.log_db_pwd).getOrElse("<use_global_properties_log_db_pwd>"))
    config.setMaximumPoolSize(2)
    HikariTransactor.fromHikariConfig[Task](config, ec, Blocker.liftExecutionContext(ec))
  }.toManagedZIO

  def createDbLogger(transactor: HikariTransactor[Task], job_name: String, job_properties: EtlJobProps): ZManaged[Any, Nothing, DbManager] =
    Task.succeed(new DbManager(transactor, job_name, job_properties)).toManaged_

  private def logError(e: Throwable): None.type = {
    db_logger.error(s"Cannot update job state to log db => ${e.getMessage}")
    None
  }

  def createDbLogger(job_name: String, job_properties: EtlJobProps, global_properties: Option[GlobalProperties], ec: ExecutionContext): Option[DbManager] = {
    Try {
      val dataSource = new HikariDataSource()
      dataSource.setDriverClassName("org.postgresql.Driver")
      dataSource.setJdbcUrl(global_properties.map(_.log_db_url).getOrElse("<use_global_properties_log_db_url>"))
      dataSource.setUsername(global_properties.map(_.log_db_user).getOrElse("<use_global_properties_log_db_user>"))
      dataSource.setPassword(global_properties.map(_.log_db_pwd).getOrElse("<use_global_properties_log_db_pwd>"))
      HikariTransactor[Task](dataSource, ec, Blocker.liftExecutionContext(ec))
    }.fold(ex => logError(ex), trans => Some(trans)).map{ transactor =>
      new DbManager(transactor,job_name, job_properties)
    }
  }
}
