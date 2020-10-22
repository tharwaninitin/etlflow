package etlflow.log

import etlflow.EtlJobProps
import etlflow.etlsteps.EtlStep
import etlflow.utils.{Config, HttpClientApi, LoggingLevel, UtilityFunctions => UF}
import zio.Task
import etlflow.utils.LoggingLevel.{DEBUG, INFO, JOB}
import zio.Runtime.global.unsafeRun

class SlackLogManager private[log] (
                                     val job_name: String,
                                     val job_properties: EtlJobProps,
                                     val web_hook_url: String = "",
                                     val env: String = "",
                                   ) extends LogManager[Unit] {
  /** Slack message templates */
  var final_step_message: String = ""
  var final_message: String = ""

  def finalMessageTemplate(run_env: String, exec_date: String, message: String, status: String): String = {
    if (status == "pass") {
      /** Template for slack success message */
      job_properties.job_notification_level match {
        case JOB => {
          final_message = final_message.concat(f"""
          :large_blue_circle: $run_env - ${job_name} Process *Success!*
          *Time of Execution*: $exec_date
          """)
          final_message
        }
        case INFO | DEBUG =>
          final_message = final_message.concat( f"""
          :large_blue_circle: $run_env - ${job_name} Process *Success!*
          *Time of Execution*: $exec_date
          *Steps (Task - Duration)*: $message
          """)
          final_message
      }
    }
    else {
      /** Template for slack failure message **/
      final_message = final_message.concat(f"""
          :red_circle: $run_env - ${job_name} Process *Failed!*
          *Time of Execution*: $exec_date
          *Steps (Task - Duration)*: $message
          """)
      final_message
    }
  }

  def updateStepLevelInformation(
                                  execution_start_time: Long, etlstep: EtlStep[_,_], state_status: String
                                  , error_message: Option[String] = None, mode: String = "update"
                                ): Unit = {
    var slackMessageForSteps = ""
    val elapsedTime = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
    val step_icon = if (state_status.toLowerCase() == "pass") "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

    // Update the slackMessageForSteps variable and get the information of step name and its execution time
    slackMessageForSteps = step_icon + "*" + etlstep.name + "*" + " - (" + elapsedTime + ")"

    // Update the slackMessageForSteps variable and get the information of etl steps properties
    val error = error_message.map(msg => f"error -> $msg").getOrElse("")
    job_properties.job_notification_level match {
      case DEBUG => slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + etlstep.getStepProperties(job_properties.job_notification_level).mkString(", ") + error_message.map(msg => f", error -> $msg").getOrElse(""))
      case INFO | JOB=> {
        if (error.isEmpty && job_properties.job_notification_level == INFO)
          slackMessageForSteps = slackMessageForSteps
        else if(error.isEmpty && job_properties.job_notification_level == JOB)
          slackMessageForSteps = ""
        else
          slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + error)
      }
    }
    // Concatenate all the messages with finalSlackMessage
    final_step_message = final_step_message.concat(slackMessageForSteps)
  }

  def updateJobInformation(execution_start_time: Long,status: String, mode: String = "update", error_message: Option[String] = None): Unit = {
    val execution_date_time = UF.getCurrentTimestampAsString("yyyy-MM-dd HH:mm:ss")

    val data = finalMessageTemplate(
      env,
      execution_date_time,
      final_step_message,
      status
    )

    if (job_properties.job_notification_level == LoggingLevel.DEBUG)
      println(data)

    unsafeRun(HttpClientApi.post(
      web_hook_url,
      Left(f""" { "text" : "$data" } """),
      Map("content-type"->"application/json"),
      log_response = false,
      connectionTimeOut =  10000,
      readTimeOut = 15000
    ).fold(
      ex  => println("Error in sending slack notification: " + ex.getMessage),
      _   => println("Sent slack notification")
    ))
  }
}

object SlackLogManager {

  def createSlackLogger(job_name: String, job_properties: EtlJobProps,env:String,slack_url:String):SlackLogManager = {
    new SlackLogManager(job_name, job_properties,slack_url, env)
  }

  def createSlackLogger(job_name: String, job_properties: EtlJobProps, global_properties: Config): Task[Option[SlackLogManager]] = Task {
    if (job_properties.job_send_slack_notification)
      Some(new SlackLogManager(job_name, job_properties, global_properties.slack.url, global_properties.slack.env))
    else
      None
  }
}