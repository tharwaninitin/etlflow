package etlflow.audit

object Sql {
  def updateTaskRun(taskRunId: String, props: String, status: String): String =
    s"""UPDATE taskrun
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
  ): String =
    s"""INSERT INTO taskrun (
           task_run_id,
           job_run_id,
           task_name,
           task_type,
           props,
           status,
           created_at,
           updated_at
           )
         VALUES ($taskRunId, $jobRunId, $name, $taskType, CAST($props as JSON), 'started', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""

  def updateJobRun(jobRunId: String, status: String, props: String): String =
    s"""UPDATE jobrun
              SET status = $status,
                  props = CAST($props as JSON),
                  updated_at = CURRENT_TIMESTAMP(6)
           WHERE job_run_id = $jobRunId"""

  def insertJobRun(jobRunId: String, name: String, args: String, props: String): String =
    s"""INSERT INTO jobrun(
            job_run_id,
            job_name,
            args,
            props,
            status,
            created_at,
            updated_at
            )
         VALUES ($jobRunId, $name, CAST($args as JSON), CAST($props as JSON), 'started', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""

}
