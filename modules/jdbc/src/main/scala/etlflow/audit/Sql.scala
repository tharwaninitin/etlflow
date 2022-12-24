package etlflow.audit

import scalikejdbc._

private[etlflow] object Sql {

  def updateTaskRun(taskRunId: String, props: String, status: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE taskrun
            SET status = $status,
                props = CAST($props as JSON),
                updated_at = CURRENT_TIMESTAMP()
          WHERE task_run_id = $taskRunId"""

  def insertTaskRun(
      taskRunId: String,
      name: String,
      props: String,
      taskType: String,
      jobRunId: String
  ): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO taskrun (
           task_run_id,
           task_name,
           props,
           status,
           elapsed_time,
           task_type,
           job_run_id,
           created_at,
           updated_at
           )
         VALUES ($taskRunId, $name, CAST($props as JSON), 'started', '...', $taskType, $jobRunId, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""

  def updateJobRun(jobRunId: String, status: String, props: String): SQL[Nothing, NoExtractor] =
    sql""" UPDATE jobrun
              SET status = $status,
                  props = CAST($props as JSON),
                  updated_at = CURRENT_TIMESTAMP()
           WHERE job_run_id = $jobRunId"""

  def insertJobRun(jobRunId: String, name: String, args: String, props: String): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO jobrun(
            job_run_id,
            job_name,
            args,
            props,
            status,
            elapsed_time,
            created_at,
            updated_at
            )
         VALUES ($jobRunId, $name, CAST($args as JSON), CAST($props as JSON), 'started', '...', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""

}
