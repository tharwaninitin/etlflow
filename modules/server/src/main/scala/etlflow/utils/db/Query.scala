package etlflow.utils.db

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.implicits._
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper.Creds.{AWS, JDBC}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.{CacheHelper, JsonJackson}
import org.slf4j.{Logger, LoggerFactory}
import pdi.jwt.{Jwt, JwtAlgorithm}
import scalacache.Cache
import zio.interop.catz._
import zio.{IO, Task}

object Query {

  lazy val query_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def login(args: UserArgs,transactor: HikariTransactor[Task],cache: Cache[String]): Task[UserAuth] =  {
    sql"""SELECT
              user_name,
              password,
              user_active,
              user_role
          FROM userinfo
          WHERE user_name = ${args.user_name} AND password = ${args.password}"""
      .query[UserInfo] // Query0[String]
      .to[List]        // Stream[ConnectionIO, String]
      .transact(transactor).flatMap(z => {
      if (z.isEmpty) {
        Task(UserAuth("Invalid User", ""))
      }
      else {
        Task {
          var user_name = ""
          var user_role = ""
          z.map(data => {
            user_name = data.user_name
            user_role = data.user_role
          })
          val user_data = s"""{"user":"$user_name", "role":"$user_role"}""".stripMargin
          val token = Jwt.encode(user_data, "secretKey", JwtAlgorithm.HS256)
          query_logger.info(s"New token generated for user $user_name")
          CacheHelper.putKey(cache,token,token,Some(CacheHelper.default_ttl))
          UserAuth("Valid User", token)
        }
      }
    })
  }

  def updateJobState(args: EtlJobStateArgs,transactor: HikariTransactor[Task]): Task[Boolean] = {
    sql"UPDATE job SET is_active = ${args.state} WHERE job_name = ${args.name}"
      .update
      .run
      .transact(transactor).map(_ => args.state)
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }


  def getCronJobFromDB(name: String, transactor: HikariTransactor[Task]): IO[ExecutionError, JobDB] = {
    sql"SELECT job_name, job_description, schedule, failed, success, is_active FROM job WHERE job_name = ${name}"
      .query[JobDB] // Query0[String]
      .to[List]        // Stream[ConnectionIO, String]
      .transact(transactor).map(x => x.head)
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def addCredentials(args: CredentialsArgs, transactor: HikariTransactor[Task]): Task[Credentials] = {
    val credentialsDB = CredentialDB(
      args.name,
      args.`type`.get match {
        case JDBC => "jdbc"
        case AWS => "aws"
      },
      JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
    )

    sql"INSERT INTO credentials (name,type,value) VALUES (${credentialsDB.name}, ${credentialsDB.`type`}, ${credentialsDB.value})"
      .update
      .run
      .transact(transactor)
      .map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateCredentials(args: CredentialsArgs,transactor: HikariTransactor[Task]): Task[Credentials] = {

    val value = JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
    sql"UPDATE credentials SET value = ${value} WHERE name = ${args.name}"
      .update.run.transact(transactor).map(_ => Credentials(args.name,"",value))

  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def addCronJob(args: CronJobArgs,transactor: HikariTransactor[Task]): Task[CronJob] = {
    val cronJobDB = JobDB(args.job_name,"", args.schedule.toString,0,0,is_active = true)
    sql"""INSERT INTO job (job_name,schedule,failed,success,is_active)
         VALUES (${cronJobDB.job_name}, ${cronJobDB.schedule}, ${cronJobDB.failed}, ${cronJobDB.success}, ${cronJobDB.is_active})"""
      .update
      .run
      .transact(transactor).map(_ => CronJob(cronJobDB.job_name,"",Cron(cronJobDB.schedule).toOption,0,0))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateCronJob(args: CronJobArgs,transactor: HikariTransactor[Task]): Task[CronJob] = {
    sql"UPDATE job SET schedule = ${args.schedule.toString} WHERE job_name = ${args.job_name}"
      .update.run.transact(transactor).map(_ => CronJob(args.job_name,"",Some(args.schedule),0,0))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def getDbStepRuns(args: DbStepRunArgs,transactor: HikariTransactor[Task]): Task[List[StepRun]] = {
    try {
      sql"""SELECT job_run_id,
              step_name,
              properties,
              state,
              start_time,
              elapsed_time,
              step_type,
              step_run_id
              FROM StepRun
            WHERE job_run_id = ${args.job_run_id} ORDER BY inserted_at DESC"""
        .query[StepRun] // Query0[String]
        .to[List]
        .transact(transactor)
    }
    catch {
      case x: Throwable =>
        query_logger.error(s"Exception occurred for arguments $args")
        x.getStackTrace.foreach(msg => query_logger.error("=> " + msg.toString))
        throw x
    }
  }

  def getDbJobRuns(args: DbJobRunArgs, transactor: HikariTransactor[Task]): Task[List[JobRun]] = {

    var q: ConnectionIO[List[JobRun]] = null

    try {
      if (args.jobRunId.isDefined && args.jobName.isEmpty) {
        q =
          sql"""SELECT job_run_id,
                  job_name,
                  properties,
                  state,
                  start_time,
                  elapsed_time,
                  job_type,
                  is_master
                FROM jobRun
                WHERE job_run_id = ${args.jobRunId.get}
                AND is_master = 'true'
                ORDER BY inserted_at DESC
                offset ${args.offset} limit ${args.limit}"""
          .query[JobRun] // Query0[String]
          .to[List]
        // logger.info(s"Query Fragment Generated for arguments $args is ")
      } else if (args.jobRunId.isEmpty && args.jobName.isDefined && args.filter.isDefined && args.endTime.isDefined) {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val startTime = if(args.startTime.get == "")
          sdf.parse(LocalDate.now().plusDays(1).toString)
        else
          args.startTime.get

        val endTime =  args.endTime.get.plusDays(1)

        args.filter.get match {
          case "IN" => {
            query_logger.info("Inside IN clause")
            q = sql"""
                   SELECT
                       job_run_id,
                       job_name,
                       properties,
                       state,
                       start_time,
                       elapsed_time,
                       job_type,
                       is_master
                   FROM jobRun
                   WHERE job_name = ${args.jobName.get}
                   AND is_master = 'true'
                   AND inserted_at::date >= ${startTime.toString}::date
                   AND inserted_at::date < ${endTime.toString}::date
                   ORDER BY inserted_at DESC
                   offset ${args.offset} limit ${args.limit}"""
              .query[JobRun]
              .to[List]
          }
          case "NOT IN" => {
            query_logger.info("Inside NOT IN  clause" )
            q = sql"""
                   SELECT
                       job_run_id,
                       job_name,
                       properties,
                       state,
                       start_time,
                       elapsed_time,
                       job_type,
                       is_master
                   FROM jobRun
                   WHERE job_name != ${args.jobName.get}
                   AND is_master = 'true'
                   AND inserted_at::date >= ${startTime.toString}::date
                   AND inserted_at::date < ${endTime.toString}::date
                   ORDER BY inserted_at DESC
                   offset ${args.offset} limit ${args.limit}"""
              .query[JobRun]
              .to[List]
          }
        }
        // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
      }
      else if (args.jobRunId.isEmpty && args.jobName.isDefined && args.filter.isDefined) {
        args.filter.get match {
          case "IN" => {
            query_logger.info("Inside IN clause")
            q = sql"""
                   SELECT
                       job_run_id,
                       job_name,
                       properties,
                       state,
                       start_time,
                       elapsed_time,
                       job_type,
                       is_master
                   FROM jobRun
                   WHERE job_name = ${args.jobName.get}
                   AND is_master = 'true'
                   ORDER BY inserted_at DESC
                   offset ${args.offset} limit ${args.limit}"""
              .query[JobRun]
              .to[List]
          }
          case "NOT IN" => {
            query_logger.info("Inside NOT IN  clause" )
            q = sql"""
                   SELECT
                       job_run_id,
                       job_name,
                       properties,
                       state,
                       start_time,
                       elapsed_time,
                       job_type,
                       is_master
                   FROM jobRun
                   WHERE job_name != ${args.jobName.get}
                   AND is_master = 'true'
                   ORDER BY inserted_at DESC
                   offset ${args.offset} limit ${args.limit}"""
              .query[JobRun]
              .to[List]
          }
        }
      }
      else if (args.endTime.isDefined) {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val startTime = if(args.startTime.get == "")
          sdf.parse(LocalDate.now().plusDays(1).toString)
        else
          args.startTime.get

        val endTime =  args.endTime.get.plusDays(1)
        q = sql"""
                   SELECT
                       job_run_id,
                       job_name,
                       properties,
                       state,
                       start_time,
                       elapsed_time,
                       job_type,
                       is_master
                   FROM jobRun
                   WHERE inserted_at::date >= ${startTime.toString}::date
                   AND inserted_at::date < ${endTime.toString}::date
                   AND is_master = 'true'
                   ORDER BY inserted_at DESC
                   offset ${args.offset} limit ${args.limit}"""
          .query[JobRun]
          .to[List]
        // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
      }
      else {
        q = sql"""
                   SELECT
                       job_run_id,
                       job_name,
                       properties,
                       state,
                       start_time,
                       elapsed_time,
                       job_type,
                       is_master
                   FROM jobRun
                   WHERE is_master = 'true'
                   ORDER BY inserted_at DESC
                   offset ${args.offset} limit ${args.limit}"""
          .query[JobRun]
          .to[List]
      }
      q.transact(transactor)
    }
    catch {
      case x: Throwable =>
        // This below code should be error free always
        query_logger.error(s"Exception occurred for arguments $args")
        x.getStackTrace.foreach(msg => query_logger.error("=> " + msg.toString))
        // Throw error here or DummyResults
        throw x
    }
  }

  def getJobs(transactor: HikariTransactor[Task]): Task[List[JobDB]] = {
    sql"SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active FROM job x"
      .query[JobDB] // Query0[String]
      .to[List]
      .transact(transactor)
  }
}
