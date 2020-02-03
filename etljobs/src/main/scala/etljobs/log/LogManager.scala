package etljobs.log

import etljobs.EtlJobName
import etljobs.etlsteps.EtlStep

// CREATE TABLE job(job_run_id varchar,job_name varchar, description varchar, properties varchar, state varchar)
case class Job(
                job_run_id: String,
                job_name: String,
                description: Option[String] = None,
                properties: Option[String] = None,
                state: Option[String] = None
              )

// CREATE TABLE step(job_run_id varchar, step_name varchar, properties varchar, state varchar, elapsed_time varchar);
case class Step(
                 job_run_id: String,
                 step_name: String,
                 properties: String,
                 state: String,
                 elapsed_time: String
               )

trait LogManager {
  var job_name: EtlJobName
  var job_run_id: String

  def finalMessageTemplate(run_env: String, job_name: EtlJobName, exec_date: String, message: String, result: String): String = ""
  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[Unit,Unit],
                                  state_status: String,
                                  notification_level:String,
                                  error_message: Option[String] = None
                                ): Unit
  def sendNotification(result: String, start_time: Long): Unit = ()
}
