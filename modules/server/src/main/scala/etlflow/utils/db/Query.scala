package etlflow.utils.db

import java.text.SimpleDateFormat
import java.time.LocalDate
import caliban.CalibanError.ExecutionError
import doobie.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.implicits._
import etlflow.log.{ApplicationLogger, JobRun, StepRun}
import etlflow.utils.EtlFlowHelper.{JobLogs, JobLogsArgs, _}
import etlflow.utils.{CacheHelper}
import pdi.jwt.{Jwt, JwtAlgorithm}
import scalacache.Cache
import zio.interop.catz._
import zio.{IO, Task}

object Query extends ApplicationLogger {

  def login(args: UserArgs,transactor: HikariTransactor[Task],cache: Cache[String]): Task[UserAuth] =  {
    val getUser =
      sql"""SELECT
              user_name,
              password,
              user_active,
              user_role
          FROM userinfo
          WHERE user_name = ${args.user_name} AND password = ${args.password}"""
        .query[UserInfo] // Query0[String]
        .to[List]        // Stream[ConnectionIO, String]
        .transact(transactor)
        .mapError { e =>
          logger.error(e.getMessage)
          ExecutionError(e.getMessage)
        }

    getUser.flatMap(z => {
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
          logger.info(s"New token generated for user $user_name")
          CacheHelper.putKey(cache,token,token,Some(CacheHelper.default_ttl))
          UserAuth("Valid User", token)
        }
      }
    })
  }

  def getJob(name: String, transactor: HikariTransactor[Task]): IO[ExecutionError, JobDB] = {
    sql"SELECT job_name, job_description, schedule, failed, success, is_active FROM job WHERE job_name = $name"
      .query[JobDB]
      .unique
      .transact(transactor)
  }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def getJobs(transactor: HikariTransactor[Task]): IO[ExecutionError, List[JobDB]] = {
    sql"SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active FROM job x"
      .query[JobDB] // Query0[String]
      .to[List]
      .transact(transactor)
      .mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
  }

  def getStepRuns(args: DbStepRunArgs,transactor: HikariTransactor[Task]): IO[ExecutionError, List[StepRun]] = {
    sql"""SELECT job_run_id,
            step_name,
            properties,
            state,
            start_time,
            elapsed_time,
            step_type,
            step_run_id
            FROM StepRun
          WHERE job_run_id = ${args.job_run_id}
          ORDER BY inserted_at DESC"""
      .query[StepRun] // Query0[String]
      .to[List]
      .transact(transactor)
      .mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
  }

  def getJobRuns(args: DbJobRunArgs, transactor: HikariTransactor[Task]): IO[ExecutionError, List[JobRun]] = {

    var q: ConnectionIO[List[JobRun]] = null

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
          logger.info("Inside IN clause")
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
          logger.info("Inside NOT IN  clause" )
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
          logger.info("Inside IN clause")
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
          logger.info("Inside NOT IN  clause" )
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
    q.transact(transactor).mapError { e =>
      logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
      ExecutionError(e.getMessage)
    }
  }

  def getJobLogs(args: JobLogsArgs, transactor: HikariTransactor[Task]): IO[ExecutionError, List[JobLogs]] = {

    var q: ConnectionIO[List[JobLogs]] = null

    if(args.filter.isDefined && args.limit.isDefined) {
      val filter = args.filter.get
      logger.info("filter :" + filter)
      q = sql"""SELECT job_name,sum(success) as success, sum(failed) as failed from (
                SELECT job_name,
                      CASE
                          WHEN state = 'pass'
                                THEN sum(count) ELSE 0
                      END success,
                      CASE
                          WHEN state != 'pass'
                                THEN sum(count) ELSE 0
                      END failed
                FROM (select job_name, state,count(*) as count from jobrun
                  WHERE start_time::timestamp::date BETWEEN now()::timestamp::date - interval ${filter} AND now()::timestamp::date
                  GROUP by job_name,state limit ${args.limit}) t
                  GROUP by job_name,state
                ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    } else if (args.filter.isDefined) {
      logger.info("inside is defines")
      q = sql"""SELECT job_name,sum(success) as success, sum(failed) as failed from (
                SELECT job_name,
                       CASE
                          WHEN state = 'pass'
                          THEN sum(count)
                          ELSE 0
                      END success,
                      CASE
                          WHEN state != 'pass'
                          THEN sum(count)
                          ELSE 0
                      END failed
               FROM (select job_name, state,count(*) as count from jobrun
                  WHERE start_time::timestamp::date BETWEEN now()::timestamp::date - interval ${args.filter.get.toString} AND now()::timestamp::date
                  GROUP by job_name,state limit 50) t
                  GROUP by job_name,state
               ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    } else if(args.limit.isDefined) {
      q = sql"""SELECT job_name,sum(success) as success, sum(failed) as failed from (
               SELECT job_name,
                      CASE
                          WHEN state = 'pass'
                          THEN sum(count)
                          ELSE 0
                      END success,
                      CASE
                          WHEN state != 'pass'
                          THEN sum(count)
                          ELSE 0
                      END failed
               FROM (select job_name, state,count(*) as count from jobrun
                  GROUP by job_name,state limit ${args.limit}) t
                  GROUP by job_name,state
               ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    } else {
      q = sql"""SELECT job_name,sum(success) as success, sum(failed) as failed from (
                 SELECT job_name,
                        CASE
                            WHEN state = 'pass'
                            THEN sum(count)
                            ELSE 0
                        END success,
                        CASE
                            WHEN state != 'pass'
                            THEN sum(count)
                            ELSE 0
                        END failed
                 FROM (select job_name, state,count(*) as count from jobrun
                    GROUP by job_name,state limit 100) t
                    GROUP by job_name,state
                 ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    }
    q.transact(transactor).mapError { e =>
      logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
      ExecutionError(e.getMessage)
    }
  }

  def getCredentials(transactor: HikariTransactor[Task]): IO[ExecutionError, List[UpdateCredentialDB]] = {
    sql"SELECT name, type ,valid_from FROM credentials WHERE valid_to is  null;"
      .query[UpdateCredentialDB]
      .to[List]
      .transact(transactor)
  }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
}
