package etljobs.log

case class StepRun(
                    job_run_id: String,
                    step_name: String,
                    properties: String,
                    state: String,
                    elapsed_time:String
                  )
