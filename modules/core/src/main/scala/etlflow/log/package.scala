package etlflow

package object log {
  case class JsonString(str: String) extends AnyVal
  case class JobRun(
                     job_run_id: String,
                     job_name: String,
                     properties: String,
                     state: String,
                     start_time: String,
                     elapsed_time: String,
                     job_type: String,
                     is_master:String
                   )
  case class StepRun(
                      job_run_id: String,
                      step_name: String,
                      properties: String,
                      state: String,
                      start_time: String,
                      elapsed_time:String,
                      step_type:String,
                      step_run_id:String
                    )
}
