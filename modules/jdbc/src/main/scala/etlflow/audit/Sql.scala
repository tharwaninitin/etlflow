package etlflow.audit

import scalikejdbc._

private[etlflow] object Sql {

  def updateTaskRun(taskRunId: String, status: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE taskrun
            SET status = $status,
                updated_at = CURRENT_TIMESTAMP(6)
          WHERE task_run_id = $taskRunId"""

  def insertTaskRun(
      taskRunId: String,
      name: String,
      metadata: String,
      taskType: String,
      jobRunId: String,
      status: String
  ): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO taskrun (
           task_run_id,
           job_run_id,
           task_name,
           task_type,
           metadata,
           status,
           created_at,
           updated_at
           )
         VALUES ($taskRunId, $jobRunId, $name, $taskType, CAST($metadata as JSON), $status, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))"""

  def updateJobRun(jobRunId: String, status: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE jobrun
              SET status = $status,
                  updated_at = CURRENT_TIMESTAMP(6)
           WHERE job_run_id = $jobRunId"""

  def insertJobRun(jobRunId: String, name: String, metadata: String, status: String): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO jobrun(
            job_run_id,
            job_name,
            metadata,
            status,
            created_at,
            updated_at
            )
         VALUES ($jobRunId, $name, CAST($metadata as JSON), $status, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))"""

}
