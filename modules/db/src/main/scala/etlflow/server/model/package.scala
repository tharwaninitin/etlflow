package etlflow.server

import scalikejdbc.{WrappedResultSet, SQLSyntaxSupport}

package object model {
  case class DbStepRunArgs(job_run_id: String)
  case class DbJobRunArgs(
      jobRunId: Option[String] = None,
      jobName: Option[String] = None,
      startTime: Option[java.time.LocalDate] = None,
      endTime: Option[java.time.LocalDate] = None,
      filter: Option[String] = None,
      limit: Long,
      offset: Long
  )

  case class JobLogsArgs(filter: Option[Double] = None, limit: Option[Long] = None)

  case class Credential(name: String, `type`: String, json: String)
  case class GetCredential(name: String, `type`: String, valid_from: String)
  object GetCredential extends SQLSyntaxSupport[GetCredential] {
    def apply(rs: WrappedResultSet): GetCredential = GetCredential(rs.string("name"), rs.string("type"), rs.string("valid_from"))
  }

  case class JobLogs(job_name: String, success: String, failed: String)
  object JobLogs extends SQLSyntaxSupport[JobLogs] {
    def apply(rs: WrappedResultSet): JobLogs = JobLogs(rs.string("job_name"), rs.string("success"), rs.string("failed"))
  }

  case class EtlJobStateArgs(name: String, state: Boolean)
  case class EtlJob(name: String, props: Map[String, String])

  case class UserDB(user_name: String, password: String, user_active: String, user_role: String)
  object UserDB extends SQLSyntaxSupport[UserDB] {
    def apply(rs: WrappedResultSet): UserDB =
      UserDB(rs.string("user_name"), rs.string("password"), rs.string("user_active"), rs.string("user_role"))
  }

  case class JobDB(job_name: String, schedule: String, is_active: Boolean)
  object JobDB extends SQLSyntaxSupport[JobDB] {
    def apply(rs: WrappedResultSet): JobDB = JobDB(rs.string("job_name"), rs.string("schedule"), rs.boolean("is_active"))
  }

  case class JobDBAll(
      job_name: String,
      job_description: String,
      schedule: String,
      failed: Long,
      success: Long,
      is_active: Boolean,
      last_run_time: Option[Long] = None
  )
  object JobDBAll extends SQLSyntaxSupport[JobDBAll] {
    override val tableName = "Job"
    def apply(rs: WrappedResultSet): JobDBAll = JobDBAll(
      rs.string("job_name"),
      rs.string("job_description"),
      rs.string("schedule"),
      rs.long("failed"),
      rs.long("success"),
      rs.boolean("is_active"),
      rs.longOpt("last_run_time")
    )
  }

  case class JobRun(
      job_run_id: String,
      job_name: String,
      properties: String,
      state: String,
      start_time: String,
      elapsed_time: String,
      job_type: String,
      is_master: String
  )
  case class JobRunDB(
      job_run_id: String,
      job_name: String,
      properties: String,
      status: String,
      elapsed_time: String,
      job_type: String,
      is_master: String,
      inserted_at: Long
  )
  object JobRunDB extends SQLSyntaxSupport[JobRunDB] {
    def apply(rs: WrappedResultSet): JobRunDB = JobRunDB(
      rs.string("job_run_id"),
      rs.string("job_name"),
      rs.string("properties"),
      rs.string("status"),
      rs.string("elapsed_time"),
      rs.string("job_type"),
      rs.string("is_master"),
      rs.long("inserted_at")
    )
  }

  case class StepRun(
      job_run_id: String,
      step_name: String,
      properties: String,
      state: String,
      start_time: String,
      elapsed_time: String,
      step_type: String,
      step_run_id: String
  )
  case class StepRunDB(
      job_run_id: String,
      step_name: String,
      properties: String,
      status: String,
      elapsed_time: String,
      step_type: String,
      step_run_id: String,
      inserted_at: Long
  )
  object StepRunDB extends SQLSyntaxSupport[StepRunDB] {
    def apply(rs: WrappedResultSet): StepRunDB = StepRunDB(
      rs.string("job_run_id"),
      rs.string("step_name"),
      rs.string("properties"),
      rs.string("status"),
      rs.string("elapsed_time"),
      rs.string("step_type"),
      rs.string("step_run_id"),
      rs.long("inserted_at")
    )
  }
}
