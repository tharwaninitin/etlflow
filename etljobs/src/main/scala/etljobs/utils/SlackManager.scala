package etljobs.utils

import etljobs.etlsteps.EtlStep
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity

/** Object SlackManager contains the information regarding
 *       - Slack message template
 *       - Update the slack message
 *       - Send the slack message to appropriate channels
 */
object SlackManager{

  private var final_slack_message = ""
  var error_occured : Boolean = false
  var job_properties : Map[String,String] = Map()
  var webhook_url: String = ""
  var env: String = ""

  /** Template for slack success message */
  private def ingestionSlackSuccessMessage(run_env:String, event:String, exec_date:String, message:String): String= {
    f"""
    :large_blue_circle: $run_env - $event Process *Success!*
    *Time of Execution*: $exec_date
    *Steps (Task - Duration)*: $message
    """
  }

  /** Template for slack failure message */
  private def ingestionSlackFailureMessage(run_env:String, event:String, exec_date:String, message:String): String= {
    f"""
    :red_circle: $run_env - $event Process *Failed!*
    *Time of Execution*: $exec_date
    *Steps (Task - Duration)*: $message
    """
  }

  /** Get the step level information and update the variable finalSlackMessage */
  def updateStepLevelInformation(execution_start_time: Long, etlstep: EtlStep[Unit,Unit], state_status: String, error_message: Option[String] = None): String = {
    var slackMessageForSteps = ""
    val execution_end_time = System.nanoTime()
    val elapsedTime = (execution_end_time - execution_start_time) / 1000000000.0 / 60.0 + " mins"
    val step_icon = if (state_status.toLowerCase() == "pass") "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

    // Update the slackMessageForSteps variable and get the information of step name and its execution time
    slackMessageForSteps = step_icon + "*" + etlstep.name + "*" + " - (" + elapsedTime + ")"
    
    // Update the slackMessageForSteps variable and get the information of etl steps properties
    slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + etlstep.getStepProperties.mkString(", ") + error_message.map(msg => f", error -> $msg").getOrElse(""))

    // Concatenate all the messages with finalSlackMessage
    final_slack_message = final_slack_message.concat(slackMessageForSteps)

    final_slack_message
  }

  /** Sends the slack notification to slack channels*/
  def sendSlackNotification(result: String, start_time: Long) : Unit = {
    val client = HttpClients.createDefault
    val slackApi = new HttpPost(webhook_url)

    val execution_date_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)

    if (result == "Pass") {
      val data = SlackManager.ingestionSlackSuccessMessage(
        env,
        job_properties("job_name"),
        execution_date_time,
        final_slack_message
      )
      val json_data = f""" { "text" : "$data" } """
      val entity = new StringEntity(json_data)
      slackApi.setEntity(entity);
      client.execute(slackApi);
    }
    else {
      val data = SlackManager.ingestionSlackFailureMessage(
        env,
        job_properties("job_name"),
        execution_date_time,
        final_slack_message
      )
      val json_data = f""" { "text" : "$data" } """
      val entity = new StringEntity(json_data)
      slackApi.setEntity(entity);
      client.execute(slackApi);
    }
  }
}