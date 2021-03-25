package etlflow.log

import java.sql.Timestamp

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
