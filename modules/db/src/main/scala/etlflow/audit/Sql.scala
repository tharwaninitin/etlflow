package etlflow.audit

import etlflow.utils.ApplicationLogger
import scalikejdbc._

private[etlflow] object Sql extends ApplicationLogger {

  def updateTaskRun(taskRunId: String, props: String, status: String, elapsedTime: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE taskrun
            SET status = $status,
                properties = CAST($props as JSON),
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
    sql"""INSERT INTO taskrun (
           task_run_id,
           task_name,
           properties,
           status,
           elapsed_time,
           task_type,
           job_run_id,
           inserted_at
           )
         VALUES ($taskRunId, $name, CAST($props as JSON), 'started', '...', $taskType, $jobRunId, $startTime)"""

  def updateJobRun(jobRunId: String, status: String, elapsedTime: String): SQL[Nothing, NoExtractor] =
    sql""" UPDATE jobrun
              SET status = $status,
                  elapsed_time = $elapsedTime
           WHERE job_run_id = $jobRunId"""

  def insertJobRun(jobRunId: String, name: String, props: String, startTime: Long): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO jobrun(
            job_run_id,
            job_name,
            properties,
            status,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES ($jobRunId, $name, CAST($props as JSON), 'started', '...', '', 'true', $startTime)"""

}
