package etljobs.log

import etljobs.{EtlJobName, EtlProps}
import etljobs.etlsteps.EtlStep
import etljobs.utils.UtilityFunctions
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import scala.util.Try

/** Object SlackManager have below functionality
 *       - Create Success and Failure Slack message templates
 *       - Send the slack message to appropriate channels
 */
object SlackManager extends LogManager with UtilityFunctions{
  override var job_properties: EtlProps = _
  var final_message = ""
  var web_hook_url: String = ""
  var env: String = ""

  /** Slack message templates */
  private def finalMessageTemplate(run_env: String, exec_date: String, message: String, status: String): String = {
    if (status == "pass") {
      /** Template for slack success message */
      return f"""
      :large_blue_circle: $run_env - ${job_properties.job_name.toString} Process *Success!*
      *Time of Execution*: $exec_date
      *Steps (Task - Duration)*: $message
      """
    }
    else {
      /** Template for slack failure message **/
      return f"""
      :red_circle: $run_env - ${job_properties.job_name.toString} Process *Failed!*
      *Time of Execution*: $exec_date
      *Steps (Task - Duration)*: $message
      """
    }
  }

  /** Get the step level information and update the variable finalSlackMessage */
  def updateStepLevelInformation(
                                  execution_start_time: Long, etlstep: EtlStep[Unit,Unit], state_status: String
                                 , notification_level:String, error_message: Option[String] = None, mode: String = "update"
                                ): Unit = {
    var slackMessageForSteps = ""
    val elapsedTime = getTimeDifferenceAsString(execution_start_time, getCurrentTimestamp)
    val step_icon = if (state_status.toLowerCase() == "pass") "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

    // Update the slackMessageForSteps variable and get the information of step name and its execution time
    slackMessageForSteps = step_icon + "*" + etlstep.name + "*" + " - (" + elapsedTime + ")"

    // Update the slackMessageForSteps variable and get the information of etl steps properties
    slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + etlstep.getStepProperties(notification_level).mkString(", ") + error_message.map(msg => f", error -> $msg").getOrElse(""))

    // Concatenate all the messages with finalSlackMessage
    final_message = final_message.concat(slackMessageForSteps)
  }

  /** Sends the slack notification to slack channels*/
  def updateJobInformation(status: String, mode: String = "update"): Unit = {
    val execution_date_time = getCurrentTimestampAsString("yyyy-MM-dd HH:mm:ss")

    val data = finalMessageTemplate(
      env,
      execution_date_time,
      final_message,
      status
    )

    if (log_level.equalsIgnoreCase("debug"))
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
