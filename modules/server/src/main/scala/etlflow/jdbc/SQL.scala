package etlflow.jdbc

import java.text.SimpleDateFormat
import java.time.LocalDate
import cats.data.NonEmptyList
import cats.effect.Async
import doobie.free.connection.ConnectionIO
import doobie.util.meta.Meta
import org.postgresql.util.PGobject
import doobie.implicits._
import etlflow.log.{JobRun, StepRun}
import etlflow.api.Schema._

object SQL {

  implicit val jsonMeta: Meta[JsonString] = Meta.Advanced.other[PGobject]("jsonb").timap[JsonString](o => JsonString(o.getValue))(a => {
    val o = new PGobject
    o.setType("jsonb")
    o.setValue(a.str)
    o
  })

  def getUser(name: String): doobie.Query0[UserInfo] =
    sql"""SELECT user_name, password, user_active, user_role FROM userinfo WHERE user_name = $name"""
      .query[UserInfo]

  def getJob(name: String): doobie.Query0[JobDB] =
    sql"SELECT job_name, schedule, is_active FROM job WHERE job_name = $name"
      .query[JobDB]

  def getJobs: doobie.Query0[JobDBAll] =
    sql"SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active, x.last_run_time FROM job x"
      .query[JobDBAll]

  def getStepRuns(args: DbStepRunArgs): doobie.Query0[StepRun] =
    sql"""SELECT job_run_id,
            step_name,
            properties::TEXT,
            state,
            start_time,
            elapsed_time,
            step_type,
            step_run_id
            FROM StepRun
          WHERE job_run_id = ${args.job_run_id}
          ORDER BY inserted_at DESC"""
      .query[StepRun]

  def getJobRuns(args: DbJobRunArgs): doobie.Query0[JobRun] = {

    var q: doobie.Query0[JobRun] = null

    if (args.jobRunId.isDefined && args.jobName.isEmpty) {
      q =
        sql"""SELECT job_run_id,
                job_name,
                properties::TEXT,
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
      // logger.info(s"Query Fragment Generated for arguments $args is ")
    }
    else if (args.jobRunId.isEmpty && args.jobName.isDefined && args.filter.isDefined && args.endTime.isDefined) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val startTime = if (args.startTime.isDefined)
        sdf.parse(args.startTime.get.toString)
      else
        sdf.parse(LocalDate.now().plusDays(1).toString)

      val endTime =  sdf.parse(args.endTime.get.plusDays(1).toString)

      args.filter.get match {
        case "IN" => {
          q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     start_time,
                     elapsed_time,
                     job_type,
                     is_master
                 FROM jobRun
                 WHERE job_name = ${args.jobName.get}
                 AND is_master = 'true'
                 AND inserted_at::date >= ${startTime}
                 AND inserted_at::date < ${endTime}
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
            .query[JobRun]
        }
        case "NOT IN" => {
          q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     start_time,
                     elapsed_time,
                     job_type,
                     is_master
                 FROM jobRun
                 WHERE job_name != ${args.jobName.get}
                 AND is_master = 'true'
                 AND inserted_at::date >= ${startTime}
                 AND inserted_at::date < ${endTime}
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
            .query[JobRun]
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
                     properties::TEXT,
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
        }
        case "NOT IN" => {
          q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
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
        }
      }
    }
    else if (args.endTime.isDefined) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val startTime = if (args.startTime.isDefined)
        sdf.parse(args.startTime.get.toString)
      else
        sdf.parse(LocalDate.now().plusDays(1).toString)

      val endTime = sdf.parse(args.endTime.get.plusDays(1).toString)
      q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     start_time,
                     elapsed_time,
                     job_type,
                     is_master
                 FROM jobRun
                 WHERE inserted_at::date >= ${startTime}
                 AND inserted_at::date < ${endTime}
                 AND is_master = 'true'
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
        .query[JobRun]
      // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
    }
    else {
      q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
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
    }
    q
  }

  def getJobLogs(args: JobLogsArgs):  doobie.Query0[JobLogs]  = {

    var q: doobie.Query0[JobLogs] = null

    if (args.filter.isDefined && args.limit.isDefined) {
      q = sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
                  WHERE start_time::timestamp::date BETWEEN (current_date - INTERVAL '1 Day' * ${args.filter.get})::date AND current_date
                    GROUP by job_name,state limit ${args.limit}) t
                    GROUP by job_name,state
                  ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
    }
    else if (args.filter.isDefined) {
      q = sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
                  WHERE start_time::timestamp::date BETWEEN (current_date - INTERVAL '1 Day' * ${args.filter.get})::date AND current_date
                  GROUP by job_name,state limit 50) t
                  GROUP by job_name,state
               ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
    }
    else if (args.limit.isDefined) {
      q = sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
    }
    else {
      q = sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
    }
    q
  }

  def getCredentials: doobie.Query0[GetCredential] =
    sql"SELECT name, type::TEXT ,valid_from::TEXT FROM credential WHERE valid_to is null;"
      .query[GetCredential]

  def updateSuccessJob(job: String, ts: Long): doobie.Update0 =
    sql"UPDATE job SET success = (success + 1), last_run_time = $ts WHERE job_name = $job"
      .update

  def updateFailedJob(job: String, ts: Long): doobie.Update0 =
    sql"UPDATE job SET failed = (failed + 1), last_run_time = $ts WHERE job_name = $job"
      .update

  def updateJobState(args: EtlJobStateArgs): doobie.Update0 =
    sql"UPDATE job SET is_active = ${args.state} WHERE job_name = ${args.name}"
      .update

  def addCredentials(args: CredentialDB): doobie.Update0 =
    sql"INSERT INTO credential (name,type,value) VALUES (${args.name}, ${args.`type`}, ${args.value})".update

  def updateCredentials(args: CredentialDB): doobie.Update0 = {
    sql"""
    UPDATE credential
    SET valid_to = NOW() - INTERVAL '00:00:01'
    WHERE credential.name = ${args.name}
       AND credential.valid_to IS NULL
    """.stripMargin.update
  }

  def updateCredentialSingleTran(args: CredentialDB): ConnectionIO[Unit]  = {

    val singleTran = for {
      _ <- updateCredentials(args).run
      _ <- addCredentials(args).run
    } yield ()

    singleTran
  }

  def deleteJobs(jobs: List[JobDB]): doobie.Update0 = {
    val list = NonEmptyList(jobs.head,jobs.tail).map(x => x.job_name)
    val query = fr"DELETE FROM job WHERE " ++ doobie.util.fragments.notIn(fr"job_name", list)
    query.update
  }

  val insertJobs: doobie.Update[JobDB] = {
    val sql = """
       INSERT INTO job AS t (job_name,job_description,schedule,failed,success,is_active)
       VALUES (?, '', ?, 0, 0, ?)
       ON CONFLICT (job_name)
       DO UPDATE SET schedule = EXCLUDED.schedule
    """
    doobie.Update[JobDB](sql)
  }

  val selectJobs: doobie.Query0[JobDB] = {
    sql"""
       SELECT job_name, schedule, is_active
       FROM job
       """
      .query[JobDB]
  }

  def refreshJobsSingleTran(args: List[JobDB]): ConnectionIO[List[JobDB]] = {
    for {
      _        <- logCIO(s"Refreshing jobs in database")
      deleted  <- deleteJobs(args).run
      _        <- logCIO(s"Deleted jobs => $deleted")
      inserted <- insertJobs.updateMany(args)
      _        <- logCIO(s"Inserted/Updated jobs => $inserted")
      dbjobs   <- selectJobs.to[List]
    } yield dbjobs
  }

  def logGeneral[F[_]: Async](print: Any): F[Unit] = Async[F].pure(logger.info(print.toString))

  def logCIO(print: Any): ConnectionIO[Unit] = logGeneral[ConnectionIO](print)

}
