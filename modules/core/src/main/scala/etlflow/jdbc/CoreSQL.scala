package etlflow.jdbc

import doobie.util.meta.Meta
import org.postgresql.util.PGobject
import doobie.implicits._

object CoreSQL {

  private case class JsonString(str: String) extends AnyVal

  implicit private val jsonMeta: Meta[JsonString] = Meta.Advanced.other[PGobject]("jsonb").timap[JsonString](o => JsonString(o.getValue))(a => {
    val o = new PGobject
    o.setType("jsonb")
    o.setValue(a.str)
    o
  })

  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): doobie.Update0 = {
    sql"""INSERT INTO JobRun(
            job_run_id,
            job_name,
            properties,
            state,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES ($job_run_id, $job_name, ${JsonString(props)}, 'started', '...', $job_type, $is_master, $start_time)"""
      .update
  }
  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): doobie.Update0 = {
    sql""" UPDATE JobRun
              SET state = $status,
                  elapsed_time = $elapsed_time
           WHERE job_run_id = $job_run_id"""
      .update
  }
  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): doobie.Update0 = {
    sql"""INSERT INTO StepRun (
            job_run_id,
            step_name,
            properties,
            state,
            elapsed_time,
            step_type,
            step_run_id,
            inserted_at
            )
          VALUES ($job_run_id, $step_name, ${JsonString(props)}, 'started', '...', $step_type, $step_run_id, $start_time)"""
      .update
  }

  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): doobie.Update0 = {
    sql"""UPDATE StepRun
            SET state = $status,
                properties = ${JsonString(props)},
                elapsed_time = $elapsed_time
          WHERE job_run_id = $job_run_id AND step_name = $step_name"""
      .update
  }
}
