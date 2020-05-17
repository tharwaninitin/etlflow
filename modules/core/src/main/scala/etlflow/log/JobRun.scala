package etlflow.log

case class JobRun(
                   job_run_id: String,
                   job_name: String,
                   description: String,
                   properties: String,
                   state: String,
                   inserted_at: Long
                 )
