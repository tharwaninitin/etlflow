package etlflow.log

import etlflow.db.Utils
import etlflow.utils.ApplicationLogger
import scalikejdbc._

private[etlflow] object Sql extends ApplicationLogger {

  def updateStepRun(step_run_id: String, props: String, status: String, elapsed_time: String): SQL[Nothing, NoExtractor] = {
    sql"""UPDATE StepRun
            SET status = $status,
                properties = ${Utils.convertToPGJson(props)},
                elapsed_time = $elapsed_time
          WHERE step_run_id = $step_run_id"""
  }

  def insertStepRun(step_run_id: String, step_name: String, props: String, step_type: String, job_run_id: String, start_time: Long): SQL[Nothing, NoExtractor] = {
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
         VALUES ($step_run_id, $step_name, ${Utils.convertToPGJson(props)}, 'started', '...', $step_type, $job_run_id, $start_time)"""
  }

  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): SQL[Nothing, NoExtractor]  = {
    sql""" UPDATE JobRun
              SET status = $status,
                  elapsed_time = $elapsed_time
           WHERE job_run_id = $job_run_id"""
  }

  def insertJobRun(job_run_id: String, job_name: String, props: String, start_time: Long): SQL[Nothing, NoExtractor]  = {
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
         VALUES ($job_run_id, $job_name,  ${Utils.convertToPGJson(props)}, 'started', '...', '', 'true', $start_time)"""
  }

}
