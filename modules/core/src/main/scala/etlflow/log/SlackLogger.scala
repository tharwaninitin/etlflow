package etlflow.log

import etlflow.etlsteps.EtlStep
import etlflow.utils.HttpRequest.HttpMethod
import etlflow.utils.LoggingLevel.{DEBUG, INFO, JOB}
import etlflow.utils.{HttpRequest, LoggingLevel, UtilityFunctions => UF}
import zio.Runtime.global.unsafeRun

class SlackLogger private[log] (job_name: String, web_hook_url: String = "", env: String = "",job_notification_level:LoggingLevel,host_url:String) extends ApplicationLogger {
  /** Slack message templates */
  var final_step_message: String = ""
  var final_message: String = ""

  private def finalMessageTemplate(exec_date: String, message: String, error_message: Option[String]): String = {
    if (error_message.isEmpty) {
      /** Template for slack success message */
      job_notification_level match {
        case JOB =>
          final_message = final_message.concat(f"""
          :large_blue_circle: $env - $job_name Process *Success!*
          *Time of Execution*: $exec_date
          *Details Available at*: $host_url
          """)
          final_message
        case INFO | DEBUG =>
          final_message = final_message.concat( f"""
          :large_blue_circle: $env - $job_name Process *Success!*
          *Time of Execution*: $exec_date
          *Details Available at*: $host_url
          *Steps (Task - Duration)*: $message
          """)
          final_message
      }
    }
    else {
      /** Template for slack failure message **/
      final_message = final_message.concat(f"""
          :red_circle: $env - $job_name Process *Failed!*
          *Time of Execution*: $exec_date
          *Details Available at*: $host_url
          *Steps (Task - Duration)*: $message
          """)
      final_message
    }
  }

  def logStepEnd(start_time: Long, etlstep: EtlStep[_,_], error_message: Option[String] = None): Unit = {
    var slackMessageForSteps = ""
    val elapsedTime = UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)
    val step_icon = if (error_message.isEmpty) "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

    // Update the slackMessageForSteps variable and get the information of step name and its execution time
    slackMessageForSteps = step_icon + "*" + etlstep.name + "*" + " - (" + elapsedTime + ")"

    // Update the slackMessageForSteps variable and get the information of etl steps properties
    val error = error_message.map(msg => f"error -> $msg").getOrElse("")
    job_notification_level match {
      case DEBUG => slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + etlstep.getStepProperties(job_notification_level).mkString(", ") + error_message.map(msg => f", error -> $msg").getOrElse(""))
      case INFO | JOB =>
        if (error.isEmpty && job_notification_level == INFO)
          slackMessageForSteps = slackMessageForSteps
        else if(error.isEmpty && job_notification_level == JOB)
          slackMessageForSteps = ""
        else
          slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + error)
    }
    // Concatenate all the messages with finalSlackMessage
    final_step_message = final_step_message.concat(slackMessageForSteps)
  }
  def logJobEnd(start_time: Long, error_message: Option[String] = None): Unit = {
    val execution_date_time = UF.getTimestampAsString(start_time)
    // Add time difference in above expression

    val data = finalMessageTemplate(
      execution_date_time,
      final_step_message,
      error_message
    )

    unsafeRun(HttpRequest.execute(
      HttpMethod.POST,
      web_hook_url,
      Left(f""" { "text" : "$data" } """),
      Map.empty,
      log = false,
      connection_timeout =  10000,
      read_timeout = 15000
    ).fold(
      ex  => logger.error("Error in sending slack notification: " + ex.getMessage),
      _   => logger.info("Sent slack notification")
    ))
  }
}

object SlackLogger {
  def apply(job_name: String, env: String, slack_url: String,job_notification_level:LoggingLevel,job_send_slack_notification:Boolean, host_url:String): Option[SlackLogger] = {
    if (job_send_slack_notification)
      Some(new SlackLogger(job_name,slack_url, env,job_notification_level,host_url))
    else
      None
  }
}