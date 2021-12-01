package etlflow.db

import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import org.postgresql.util.PGobject
import scalikejdbc._
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}

private[etlflow] object Sql extends ApplicationLogger {

  def getStartTime(startTime:Option[java.time.LocalDate]): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    if (startTime.isDefined)
      startTime.get.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
    else
      sdf.parse(LocalDate.now().toString).getTime
  }

  def JsonConverter(rs: String): PGobject = {
    val jsonObject = new PGobject()
    jsonObject.setType("json")
    jsonObject.setValue(rs)
    jsonObject
  }

  def getUser(name: String): SQL[Nothing, NoExtractor] =
    sql"""SELECT user_name, password, user_active, user_role FROM userinfo WHERE user_name = $name"""

  def getCredentialsWithFilter(credential_name: String):  SQL[Nothing, NoExtractor] =
    sql"""SELECT value FROM credential WHERE name='$credential_name' and valid_to is null"""

  def getJob(name: String):  SQL[Nothing, NoExtractor] =
    sql"SELECT job_name, schedule, is_active FROM job WHERE job_name = $name"

  def getJobs: SQL[Nothing, NoExtractor] = sql"SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active, x.last_run_time FROM job x"

  def getStepRuns(job_run_id: String):  SQL[Nothing, NoExtractor] =
    sql"""SELECT job_run_id,
            step_name,
            properties::TEXT,
            state,
            elapsed_time,
            step_type,
            step_run_id,
            inserted_at
            FROM StepRun
          WHERE job_run_id = $job_run_id
          ORDER BY inserted_at DESC"""

  def getJobRuns(args: DbJobRunArgs): SQL[Nothing, NoExtractor] = {

    var q: SQL[Nothing, NoExtractor] = null

    if (args.jobRunId.isEmpty && args.jobName.isDefined && args.filter.isDefined && args.endTime.isDefined) {
      val startTime = getStartTime(args.startTime)
      val endTime = args.endTime.get.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
      args.filter.get match {
        case "IN" => {
          q =
            sql"""
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
        }
        case "NOT IN" => {
          q =
            sql"""
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
        }
      }
    }
    else if (args.jobRunId.isEmpty && args.jobName.isDefined && args.filter.isDefined) {
      args.filter.get match {
        case "IN" => {
          q =
            sql"""
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
        }
        case "NOT IN" => {
          q =
            sql"""
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
        }
      }
    }
    else if (args.endTime.isDefined) {
      val startTime = getStartTime(args.startTime)
      val endTime = args.endTime.get.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
      q =
        sql"""
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
    }
    else {
      q =
        sql"""
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
    }
    q
  }

  def getJobLogs(args: JobLogsArgs): SQL[Nothing, NoExtractor] = {
    var q: SQL[Nothing, NoExtractor] = null

    if (args.filter.isDefined && args.limit.isDefined) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val end_time1 = sdf.parse(LocalDate.now().minusDays(args.filter.get.toLong).toString).getTime
      val end_time2 = getCurrentTimestamp
      q =
        sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
    }
    else if (args.filter.isDefined) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val end_time1 = sdf.parse(LocalDate.now().minusDays(args.filter.get.toLong).toString).getTime
      val end_time2 = getCurrentTimestamp
      q =
        sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
    }
    else if (args.limit.isDefined) {
      q =
        sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
    }
    else {
      q =
        sql"""SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from (
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
    }
    q
  }

  def getCredentials:  SQL[Nothing, NoExtractor] =
    sql"SELECT name, type::TEXT ,valid_from::TEXT FROM credential WHERE valid_to is null;"

  def updateSuccessJob(job: String, ts: Long):  SQL[Nothing, NoExtractor] =
    sql"UPDATE job SET success = (success + 1), last_run_time = $ts WHERE job_name = $job"

  def updateFailedJob(job: String, ts: Long) :  SQL[Nothing, NoExtractor] =
    sql"UPDATE job SET failed = (failed + 1), last_run_time = $ts WHERE job_name = $job"

  def updateJobState(args: EtlJobStateArgs) :  SQL[Nothing, NoExtractor] =
    sql"UPDATE job SET is_active = ${args.state} WHERE job_name = ${args.name}"

  def addCredentials(args: CredentialDB, actualSerializerOutput: JsonString): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO credential (name,type,value) VALUES (${args.name}, ${args.`type`}, ${JsonConverter(actualSerializerOutput.str)})"""

  def updateCredentials(args: CredentialDB):SQL[Nothing, NoExtractor] =
    sql"""
    UPDATE credential
    SET valid_to = NOW() - INTERVAL '00:00:01'
    WHERE credential.name = ${args.name}
       AND credential.valid_to IS NULL
    """.stripMargin

  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): SQL[Nothing, NoExtractor] = {
    sql"""UPDATE StepRun
            SET state = $status,
                properties = ${JsonConverter(props)},
                elapsed_time = $elapsed_time
          WHERE job_run_id = $job_run_id AND step_name = $step_name"""
  }

  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): SQL[Nothing, NoExtractor] = {
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
         VALUES ($job_run_id, $step_name, ${JsonConverter(props)}, 'started', '...', $step_type, $step_run_id, $start_time)"""
  }

  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): SQL[Nothing, NoExtractor]  = {
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
         VALUES ($job_run_id, $job_name,  ${JsonConverter(props)}, 'started', '...', $job_type, $is_master, $start_time)"""
  }

  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): SQL[Nothing, NoExtractor]  = {
    sql""" UPDATE JobRun
              SET state = $status,
                  elapsed_time = $elapsed_time
           WHERE job_run_id = $job_run_id"""
  }

  def deleteJobs(jobs: List[JobDB]): SQL[Nothing, NoExtractor]  = {
    val list = jobs.map(x => x.job_name)
    sql"""DELETE FROM job WHERE job_name not in ($list)"""
  }

  def insertJobs(data: Seq[Seq[Any]]): SQL[scalikejdbc.UpdateOperation, NoExtractor] = withSQL {
    insert.into(JobDBAll)
    .multipleValues(data:_*)
    .append(sqls"ON CONFLICT(job_name) DO UPDATE SET schedule = EXCLUDED.schedule")
  }

  val selectJobs: SQL[Nothing, NoExtractor] = sql"""SELECT job_name, schedule, is_active FROM job"""
}
