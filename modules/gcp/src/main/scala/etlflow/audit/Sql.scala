package etlflow.audit

object Sql {
  def updateTaskRun(taskRunId: String, status: String): String =
    s"""UPDATE etlflow.taskrun
            SET status = "$status",
                updated_at = CURRENT_TIMESTAMP()
          WHERE task_run_id = "$taskRunId""""

  def insertTaskRun(
      taskRunId: String,
      name: String,
      metadata: String,
      taskType: String,
      jobRunId: String,
      status: String
  ): String =
    s"""INSERT INTO etlflow.taskrun (
           task_run_id,
           job_run_id,
           task_name,
           task_type,
           metadata,
           status,
           created_at,
           updated_at
           )
         VALUES ("$taskRunId", "$jobRunId", "$name", "$taskType", JSON '$metadata', $status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""

  def updateJobRun(jobRunId: String, status: String): String =
    s"""UPDATE etlflow.jobrun
              SET status = "$status",
                  updated_at = CURRENT_TIMESTAMP()
           WHERE job_run_id = "$jobRunId""""

  def insertJobRun(jobRunId: String, name: String, metadata: String, status: String): String =
    s"""INSERT INTO etlflow.jobrun(
            job_run_id,
            job_name,
            metadata,
            status,
            created_at,
            updated_at
            )
         VALUES ("$jobRunId", "$name", JSON '$metadata', $status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""

}
