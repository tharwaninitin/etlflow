package etlflow.log

import cats.effect.Blocker
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import doobie.util.fragment.Fragment
import etlflow.EtlJobProps
import etlflow.etlsteps.EtlStep
import etlflow.jdbc.DbManager
import etlflow.utils.{Config, JsonJackson, UtilityFunctions => UF}
import io.getquill.Literal
import zio.interop.catz._
import zio.{Managed, Task, ZManaged}
import scala.concurrent.ExecutionContext
import scala.util.Try
import etlflow.utils.{UtilityFunctions => UF}

class DbLogManager(val transactor: HikariTransactor[Task],val job_name: String, val job_properties: EtlJobProps) extends LogManager[Task[Long]] {

  private val ctx = new DoobieContext.Postgres(Literal) // Literal naming scheme
  import ctx._
  val remoteStep = List("EtlFlowJobStep")

  def getCredentials[T : Manifest](name: String): Task[T] = {
    val query = s"SELECT value FROM credentials WHERE name='$name';"
    for {
      result <- Fragment.const(query).query[String].unique.transact(transactor)
      op     <- Task(JsonJackson.convertToObject[T](result))
    } yield op
  }

  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[_,_],
                                  state_status: String,
                                  error_message: Option[String] = None,
                                  mode: String = "update"
                                ): Task[Long] = {

    val formatted_step_name = UF.stringFormatter(etl_step.name)
    if (mode == "insert") {
      val step = StepRun(
        job_properties.job_run_id,
        formatted_step_name,
        JsonJackson.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level)),
        state_status.toLowerCase(),
        UF.getCurrentTimestampAsString(), UF.getCurrentTimestamp,
        "...",
        etl_step.step_type,
        if(remoteStep.contains(etl_step.step_type)) etl_step.getStepProperties(job_properties.job_notification_level).get("step_run_id").get else ""
      )
      lm_logger.info(s"Inserting step info for ${formatted_step_name} in db with status => ${state_status.toLowerCase()}")
      val x: ConnectionIO[Long] = ctx.run(quote {
        query[StepRun].insert(lift(step))
      })
      val y: Task[Long] = x.transact(transactor).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
      y
    }
    else {
      val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
      val elapsed_time = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
      lm_logger.info(s"Updating step info for ${formatted_step_name} in db with status => $status")
      ctx.run(quote {
        query[StepRun]
          .filter(x => x.job_run_id == lift(job_properties.job_run_id) && x.step_name == lift(formatted_step_name))
          .update(
            _.state -> lift(status),
            _.properties -> lift(JsonJackson.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level))),
            _.elapsed_time -> lift(elapsed_time)
          )
      }).transact(transactor).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
  }

  def updateJobInformation(execution_start_time: Long,status: String, mode: String = "update", job_type:String,error_message: Option[String] = None): Task[Long] = {
    import ctx._
    if (mode == "insert") {
      val job = JobRun(
        job_properties.job_run_id, job_name.toString,
        job_properties.job_description,
        JsonJackson.convertToJsonByRemovingKeys(job_properties, List("job_run_id","job_description","job_properties","job_aggregate_error")),
        "started",
        UF.getCurrentTimestampAsString(),
        UF.getCurrentTimestamp,
        "...",
        job_type
      )
      lm_logger.info(s"Inserting job info in db with status => $status")
      ctx.run(quote {
        query[JobRun].insert(lift(job))
      }).transact(transactor).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
    else {
      lm_logger.info(s"Updating job info in db with status => $status")
      val job_status = if (error_message.isDefined) status.toLowerCase() + " with error: " + error_message.get else status.toLowerCase()
      val elapsed_time = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
      ctx.run(quote {
        query[JobRun].filter(_.job_run_id == lift(job_properties.job_run_id))
          .update(
            _.state -> lift(job_status),
            _.elapsed_time -> lift(elapsed_time)
          )
      }).transact(transactor).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
  }
}

object DbLogManager extends DbManager{

  def createDbLoggerManaged(transactor: HikariTransactor[Task], job_name: String, job_properties: EtlJobProps): ZManaged[Any, Nothing, DbLogManager] =
    Task.succeed(new DbLogManager(transactor, job_name, job_properties)).toManaged_

  def createDbLoggerOption(transactor: HikariTransactor[Task], job_name: String, job_properties: EtlJobProps): Option[DbLogManager] =
    Try(new DbLogManager(transactor,job_name, job_properties)).toOption

  def createOptionDbTransactorManagedGP(
                                         global_properties: Config,
                                         ec: ExecutionContext,
                                         blocker: Blocker,
                                         pool_name: String = "LoggerPool",
                                         job_name: String,
                                         job_properties: EtlJobProps,
                                       ): Managed[Throwable, Option[DbLogManager]] =
    if (job_properties.job_enable_db_logging) {
      createDbTransactorManaged(global_properties.dbLog,ec,pool_name)(blocker)
        .map { transactor =>
          Some(new DbLogManager(transactor, job_name, job_properties))
        }
    } else {
      Managed.unit.map(_ => None)
    }
}
