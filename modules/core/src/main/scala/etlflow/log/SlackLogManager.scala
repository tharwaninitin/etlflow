package etlflow.log

import etlflow.EtlJobProps
import etlflow.etlsteps.EtlStep
import etlflow.utils.{GlobalProperties, UtilityFunctions => UF}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import zio.Task
import scala.util.Try

class SlackLogManager private[log] (
                                  val job_name: String,
                                  val job_properties: EtlJobProps,
                                  var final_message: String = "",
                                  web_hook_url: String = "",
                                  env: String = ""
                                ) extends LogManager[Unit] {
  /** Slack message templates */
  private def finalMessageTemplate(run_env: String, exec_date: String, message: String, status: String): String = {
    if (status == "pass") {
      /** Template for slack success message */
      return f"""
      :large_blue_circle: $run_env - ${job_name} Process *Success!*
      *Time of Execution*: $exec_date
      *Steps (Task - Duration)*: $message
      """
    }
    else {
      /** Template for slack failure message **/
      return f"""
      :red_circle: $run_env - ${job_name} Process *Failed!*
      *Time of Execution*: $exec_date
      *Steps (Task - Duration)*: $message
      """
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
    slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + etlstep.getStepProperties(job_properties.job_notification_level).mkString(", ") + error_message.map(msg => f", error -> $msg").getOrElse(""))

    // Concatenate all the messages with finalSlackMessage
    final_message = final_message.concat(slackMessageForSteps)
  }

  def updateJobInformation(status: String, mode: String = "update", error_message: Option[String] = None): Unit = {
    val execution_date_time = UF.getCurrentTimestampAsString("yyyy-MM-dd HH:mm:ss")

    val data = finalMessageTemplate(
      env,
      execution_date_time,
      final_message,
      status
    )

    if (job_properties.job_notification_level.equalsIgnoreCase("debug"))
      println(data)

    Try {
      val client = HttpClients.createDefault
      val slackApi = new HttpPost(web_hook_url)
      val json_data = f""" { "text" : "$data" } """
      val entity = new StringEntity(json_data)
      slackApi.setEntity(entity);
      client.execute(slackApi);
    }
  }
}

object SlackLogManager {
  def createSlackLogger(job_name: String, job_properties: EtlJobProps, global_properties: Option[GlobalProperties]): Task[Option[SlackLogManager]] = Task {
    if (job_properties.job_send_slack_notification)
      Some(new SlackLogManager(job_name, job_properties,"",
        global_properties match {
          case Some(x) => x.slack_webhook_url
          case None => "<use_global_properties_slack_webhook_url>"
        },
        global_properties match {
          case Some(x) => x.slack_env
          case None => "<use_global_properties_slack_env>"
        }
      ))
    else
      None
  }
}