package etlflow.log

import etlflow.utils.ApplicationLogger
import scalikejdbc._

private[etlflow] object Sql extends ApplicationLogger {

  def updateTaskRun(taskRunId: String, props: String, status: String, elapsedTime: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE TaskRun
            SET status = $status,
                properties = $props::jsonb,
                elapsed_time = $elapsedTime
          WHERE task_run_id = $taskRunId"""

  def insertTaskRun(
      taskRunId: String,
      name: String,
      props: String,
      taskType: String,
      jobRunId: String,
      startTime: Long
  ): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO TaskRun (
           task_run_id,
           task_name,
           properties,
           status,
           elapsed_time,
           task_type,
           job_run_id,
           inserted_at
           )
         VALUES ($taskRunId, $name, $props::jsonb, 'started', '...', $taskType, $jobRunId, $startTime)"""

  def updateJobRun(jobRunId: String, status: String, elapsedTime: String): SQL[Nothing, NoExtractor] =
    sql""" UPDATE JobRun
              SET status = $status,
                  elapsed_time = $elapsedTime
           WHERE job_run_id = $jobRunId"""

  def insertJobRun(jobRunId: String, name: String, props: String, startTime: Long): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO JobRun(
            job_run_id,
            job_name,
            properties,
            status,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES ($jobRunId, $name, $props::jsonb, 'started', '...', '', 'true', $startTime)"""

}
