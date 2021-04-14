package etlflow.jdbc

import java.text.SimpleDateFormat
import java.time.LocalDate
import doobie.free.connection.ConnectionIO
import etlflow.utils.EtlFlowHelper._
import doobie.implicits._
import etlflow.log.{JobRun, StepRun}

object SQL {

  def getUser(name: String): ConnectionIO[UserInfo] =
    sql"""SELECT user_name, password, user_active, user_role FROM userinfo WHERE user_name = $name"""
      .query[UserInfo]
      .unique

  def getJob(name: String): ConnectionIO[JobDB] =
    sql"SELECT job_name, job_description, schedule, failed, success, is_active FROM job WHERE job_name = $name"
      .query[JobDB]
      .unique

  def getJobs: ConnectionIO[List[JobDB1]] =
    sql"SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active, x.last_run_time FROM job x"
      .query[JobDB1]
      .to[List]

  def getStepRuns(args: DbStepRunArgs): ConnectionIO[List[StepRun]] =
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
      .query[StepRun]
      .to[List]

  def getJobRuns(args: DbJobRunArgs): ConnectionIO[List[JobRun]] = {

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
      val startTime = if (args.startTime.get == "")
        sdf.parse(LocalDate.now().plusDays(1).toString)
      else
        args.startTime.get

      val endTime = args.endTime.get.plusDays(1)

      args.filter.get match {
        case "IN" => {
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
      val startTime = if (args.startTime.get == "")
        sdf.parse(LocalDate.now().plusDays(1).toString)
      else
        args.startTime.get

      val endTime = args.endTime.get.plusDays(1)
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

    q
  }

  def getJobLogs(args: JobLogsArgs): ConnectionIO[List[JobLogs]] = {

    var q: ConnectionIO[List[JobLogs]] = null

    if (args.filter.isDefined && args.limit.isDefined) {
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
                    WHERE start_time::timestamp::date BETWEEN current_date - ${args.filter} AND current_date
                    GROUP by job_name,state limit ${args.limit}) t
                    GROUP by job_name,state
                  ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    } else if (args.filter.isDefined) {
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
                  WHERE start_time::timestamp::date BETWEEN current_date - ${args.filter} AND current_date
                  GROUP by job_name,state limit 50) t
                  GROUP by job_name,state
               ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    } else if (args.limit.isDefined) {
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
                    GROUP by job_name,state limit 20) t
                    GROUP by job_name,state
                 ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
        .to[List]
    }
    q
  }

  def getCredentials: ConnectionIO[List[UpdateCredentialDB]] =
    sql"SELECT name, type::TEXT ,valid_from FROM credentials WHERE valid_to is null;"
      .query[UpdateCredentialDB]
      .to[List]
}
