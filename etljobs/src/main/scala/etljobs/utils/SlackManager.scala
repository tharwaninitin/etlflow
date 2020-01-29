package etljobs.utils

import etljobs.etlsteps.EtlStep
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity
import scala.util.Try

/** Object SlackManager have below functionality
 *       - Create Success and Failure Slack message templates
 *       - Send the slack message to appropriate channels
 */
object SlackManager{

  var final_slack_message = ""
  var job_properties : Map[String,String] = Map()
  var webhook_url: String = ""
  var env: String = ""

  /** Slack message templates */
  private def SlackMessageTemplate(run_env: String, event: String, exec_date: String, message: String, result: String): String= {
    if (result == "Pass") {
      /** Template for slack success message */
      return f"""
      :large_blue_circle: $run_env - $event Process *Success!*
      *Time of Execution*: $exec_date
      *Steps (Task - Duration)*: $message
      """
    }
    else {
      /** Template for slack failure message **/
      return f"""
      :red_circle: $run_env - $event Process *Failed!*
      *Time of Execution*: $exec_date
      *Steps (Task - Duration)*: $message
      """
    }
  }

  /** Get the step level information and update the variable finalSlackMessage */
  def updateStepLevelInformation(execution_start_time: Long, etlstep: EtlStep[Unit,Unit], state_status: String, slack_notification_level:String, error_message: Option[String] = None): String = {
    var slackMessageForSteps = ""
    val execution_end_time = System.nanoTime()
    val elapsedTime = (execution_end_time - execution_start_time) / 1000000000.0 / 60.0 + " mins"
    val step_icon = if (state_status.toLowerCase() == "pass") "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

    // Update the slackMessageForSteps variable and get the information of step name and its execution time
    slackMessageForSteps = step_icon + "*" + etlstep.name + "*" + " - (" + elapsedTime + ")"
    
    // Update the slackMessageForSteps variable and get the information of etl steps properties
    slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + etlstep.getStepProperties(slack_notification_level).mkString(", ") + error_message.map(msg => f", error -> $msg").getOrElse(""))

    // Concatenate all the messages with finalSlackMessage
    final_slack_message = final_slack_message.concat(slackMessageForSteps)

    final_slack_message
  }

  /** Sends the slack notification to slack channels*/
  def sendSlackNotification(result: String, start_time: Long) : Unit = {
    val execution_date_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
    
    val data = SlackManager.SlackMessageTemplate(
      env,
      job_properties("job_name"),
      execution_date_time,
      final_slack_message,
      result
    )
    println(data)
    
    Try {
      val client = HttpClients.createDefault
      val slackApi = new HttpPost(webhook_url)
      val json_data = f""" { "text" : "$data" } """
      val entity = new StringEntity(json_data)
      slackApi.setEntity(entity);
      client.execute(slackApi);
    }
  }
}