package etlflow.db

import cats.data.NonEmptyList
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.meta.Meta
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeFunctions.getCurrentTimestamp
import org.postgresql.util.PGobject

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}

private[db] object SQL extends ApplicationLogger {

  implicit val jsonMeta: Meta[JsonString] = Meta.Advanced.other[PGobject]("jsonb").timap[JsonString](o => JsonString(o.getValue))(a => {
    val o = new PGobject
    o.setType("jsonb")
    o.setValue(a.str)
    o
  })

  def getUser(name: String): doobie.Query0[UserDB] =
    sql"""SELECT user_name, password, user_active, user_role FROM userinfo WHERE user_name = $name"""
      .query[UserDB]

  def getJob(name: String): doobie.Query0[JobDB] =
    sql"SELECT job_name, schedule, is_active FROM job WHERE job_name = $name"
      .query[JobDB]

  def getJobs: doobie.Query0[JobDBAll] =
    sql"SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active, x.last_run_time FROM job x"
      .query[JobDBAll]

  def getStepRuns(args: DbStepRunArgs): doobie.Query0[StepRunDB] =
    sql"""SELECT job_run_id,
            step_name,
            properties::TEXT,
            state,
            elapsed_time,
            step_type,
            step_run_id,
            inserted_at
            FROM StepRun
          WHERE job_run_id = ${args.job_run_id}
          ORDER BY inserted_at DESC"""
      .query[StepRunDB]

  private def getStartTime(startTime:Option[java.time.LocalDate]): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    if (startTime.isDefined)
      startTime.get.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
    else
      sdf.parse(LocalDate.now().toString).getTime
  }

  def getJobRuns(args: DbJobRunArgs): doobie.Query0[JobRunDB] = {

    var q: doobie.Query0[JobRunDB] = null

    if (args.jobRunId.isEmpty && args.jobName.isDefined && args.filter.isDefined && args.endTime.isDefined) {
      val startTime = getStartTime(args.startTime)
      val endTime =  args.endTime.get.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
      args.filter.get match {
        case "IN" => {
          q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     elapsed_time,
                     job_type,
                     is_master,
                     inserted_at
                 FROM jobRun
                 WHERE job_name = ${args.jobName.get}
                 AND is_master = 'true'
                 AND inserted_at >= $startTime
                 AND inserted_at < $endTime
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
            .query[JobRunDB]
        }
        case "NOT IN" => {
          q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     elapsed_time,
                     job_type,
                     is_master,
                     inserted_at
                 FROM jobRun
                 WHERE job_name != ${args.jobName.get}
                 AND is_master = 'true'
                 AND inserted_at >= ${startTime}
                 AND inserted_at < ${endTime}
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
            .query[JobRunDB]
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
                     elapsed_time,
                     job_type,
                     is_master,
                     inserted_at
                 FROM jobRun
                 WHERE job_name = ${args.jobName.get}
                 AND is_master = 'true'
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
            .query[JobRunDB]
        }
        case "NOT IN" => {
          q = sql"""
                 SELECT
                   job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     elapsed_time,
                     job_type,
                     is_master,
                     inserted_at
                 FROM jobRun
                 WHERE job_name != ${args.jobName.get}
                 AND is_master = 'true'
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
            .query[JobRunDB]
        }
      }
    }
    else if (args.endTime.isDefined) {
      val startTime = getStartTime(args.startTime)
      val endTime =  args.endTime.get.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
      q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     elapsed_time,
                     job_type,
                     is_master,
                     inserted_at
                 FROM jobRun
                 WHERE inserted_at >= $startTime
                 AND inserted_at < $endTime
                 AND is_master = 'true'
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
        .query[JobRunDB]
      // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
    }
    else {
      q = sql"""
                 SELECT
                     job_run_id,
                     job_name,
                     properties::TEXT,
                     state,
                     elapsed_time,
                     job_type,
                     is_master,
                     inserted_at
                 FROM jobRun
                 WHERE is_master = 'true'
                 ORDER BY inserted_at DESC
                 offset ${args.offset} limit ${args.limit}"""
        .query[JobRunDB]
    }
    q
  }

  def getJobLogs(args: JobLogsArgs):  doobie.Query0[JobLogs]  = {

    var q: doobie.Query0[JobLogs] = null

    if (args.filter.isDefined && args.limit.isDefined) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val end_time1 = sdf.parse(LocalDate.now().minusDays(args.filter.get.toLong).toString).getTime
      val end_time2 = getCurrentTimestamp
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
                  WHERE inserted_at >= ${end_time1} AND
                    inserted_at <= ${end_time2}
                    GROUP by job_name,state limit ${args.limit}) t
                    GROUP by job_name,state
                  ) t1 GROUP by job_name;""".stripMargin
        .query[JobLogs] // Query0[String]
    }
    else if (args.filter.isDefined) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val end_time1 = sdf.parse(LocalDate.now().minusDays(args.filter.get.toLong).toString).getTime
      val end_time2 = getCurrentTimestamp
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
                  WHERE inserted_at >= ${end_time1} AND
                   inserted_at <= ${end_time2}
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

  def addCredentials(args: CredentialDB, actualSerializerOutput: JsonString): doobie.Update0 = {
    sql"INSERT INTO credential (name,type,value) VALUES (${args.name}, ${args.`type`}, ${actualSerializerOutput})".update
  }

  def updateCredentials(args: CredentialDB): doobie.Update0 = {
    sql"""
    UPDATE credential
    SET valid_to = NOW() - INTERVAL '00:00:01'
    WHERE credential.name = ${args.name}
       AND credential.valid_to IS NULL
    """.stripMargin.update
  }

  def updateCredentialSingleTran(args: CredentialDB, actualSerializerOutput: JsonString): ConnectionIO[Unit]  = {

    val singleTran = for {
      _ <- updateCredentials(args).run
      _ <- addCredentials(args, actualSerializerOutput).run
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
      deleted  <- deleteJobs(args).run
      _        = logger.info(s"Deleted jobs => $deleted")
      inserted <- insertJobs.updateMany(args)
      _        = logger.info(s"Inserted/Updated jobs => $inserted")
      dbjobs   <- selectJobs.to[List]
    } yield dbjobs
  }

//  def logGeneral[F[_]: Async](print: Any): F[Unit] = Async[F].pure(logger.info(print.toString))
//
//  def logCIO(print: Any): ConnectionIO[Unit] = logGeneral[ConnectionIO](print)

  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): doobie.Update0 = {
    sql"""INSERT INTO JobRun(
            job_run_id,
            job_name,
            properties,
            state,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES ($job_run_id, $job_name, ${JsonString(props)}, 'started', '...', $job_type, $is_master, $start_time)"""
      .update
  }

  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): doobie.Update0 = {
    sql""" UPDATE JobRun
              SET state = $status,
                  elapsed_time = $elapsed_time
           WHERE job_run_id = $job_run_id"""
      .update
  }

  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): doobie.Update0 = {
    sql"""INSERT INTO StepRun (
            job_run_id,
            step_name,
            properties,
            state,
            elapsed_time,
            step_type,
            step_run_id,
            inserted_at
            )
          VALUES ($job_run_id, $step_name, ${JsonString(props)}, 'started', '...', $step_type, $step_run_id, $start_time)"""
    .update
  }

  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): doobie.Update0 = {
    sql"""UPDATE StepRun
            SET state = $status,
                properties = ${JsonString(props)},
                elapsed_time = $elapsed_time
          WHERE job_run_id = $job_run_id AND step_name = $step_name"""
      .update
  }
}
