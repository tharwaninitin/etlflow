package etlflow.log

case class StepRun(
                    job_run_id: String,
                    step_name: String,
                    properties: String,
                    state: String,
                    start_time: String,
                    inserted_at: Long,
                    elapsed_time:String,
                    step_type:String
                  )
