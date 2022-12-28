package etlflow.audit

import scalikejdbc._

private[etlflow] object Sql {

  def updateTaskRun(taskRunId: String, props: String, status: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE taskrun
            SET status = $status,
                props = CAST($props as JSON),
                updated_at = CURRENT_TIMESTAMP(6)
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
           job_run_id,
           task_name,
           task_type,
           props,
           status,
           created_at,
           updated_at
           )
         VALUES ($taskRunId, $jobRunId, $name, $taskType, CAST($props as JSON), 'started', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))"""

  def updateJobRun(jobRunId: String, status: String, props: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE jobrun
              SET status = $status,
                  props = CAST($props as JSON),
                  updated_at = CURRENT_TIMESTAMP(6)
           WHERE job_run_id = $jobRunId"""

  def insertJobRun(jobRunId: String, name: String, args: String, props: String): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO jobrun(
            job_run_id,
            job_name,
            args,
            props,
            status,
            created_at,
            updated_at
            )
         VALUES ($jobRunId, $name, CAST($args as JSON), CAST($props as JSON), 'started', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))"""

}