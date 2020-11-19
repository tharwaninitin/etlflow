package etlflow.log

case class JobRun(
                   job_run_id: String,
                   job_name: String,
                   description: String,
                   properties: String,
                   state: String,
                   start_time: String,
                   inserted_at: Long,
                   elapsed_time: String,
                   job_type: String,
                   is_master:String
                 )
