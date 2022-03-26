package etlflow.log

import etlflow.utils.ApplicationLogger
import scalikejdbc._

private[etlflow] object Sql extends ApplicationLogger {

  def updateStepRun(stepRunId: String, props: String, status: String, elapsedTime: String): SQL[Nothing, NoExtractor] =
    sql"""UPDATE StepRun
            SET status = $status,
                properties = $props::jsonb,
                elapsed_time = $elapsedTime
          WHERE step_run_id = $stepRunId"""

  def insertStepRun(
      stepRunId: String,
      name: String,
      props: String,
      stepType: String,
      jobRunId: String,
      startTime: Long
  ): SQL[Nothing, NoExtractor] =
    sql"""INSERT INTO StepRun (
           step_run_id,
           step_name,
           properties,
           status,
           elapsed_time,
           step_type,
           job_run_id,
           inserted_at
           )
         VALUES ($stepRunId, $name, $props::jsonb, 'started', '...', $stepType, $jobRunId, $startTime)"""

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
