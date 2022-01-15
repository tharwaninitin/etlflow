package etlflow.log

import etlflow.schema
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString, getTimestampAsString}
import zio.{Task, ULayer, ZIO, ZLayer}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import scala.util.Try

object Slack extends ApplicationLogger {

  case class SlackLogger(job_run_id: String, slack: Option[schema.Slack]) extends Service {
    var final_step_message: String = ""
    var final_message: String      = ""
    val slack_env: String          = slack.map(_.env).getOrElse("")
    val slack_url: String          = slack.map(_.url).getOrElse("")
    val host_url: String           = slack.map(_.host).getOrElse("http://localhost:8080/#") + "/JobRunDetails/"

    private def finalMessageTemplate(
        job_name: String,
        exec_date: String,
        message: String,
        url: String,
        error: Option[Throwable]
    ): String =
      if (error.isEmpty) {

        /** Template for slack success message */
        final_message = final_message.concat(f"""
            :large_blue_circle: $slack_env - $job_name Process *Success!*
            *Time of Execution*: $exec_date
            *Details Available at*: $url
            *Steps (Task - Duration)*: $message
            """)
        final_message
      } else {

        /** Template for slack failure message * */
        final_message = final_message.concat(f"""
            :red_circle: $slack_env - $job_name Process *Failed!*
            *Time of Execution*: $exec_date
            *Details Available at*: $url
            *Steps (Task - Duration)*: $message
            """)
        final_message
      }
    private def sendSlackNotification(data: String): Unit =
      Try {
        val conn = new URL(slack_url)
          .openConnection()
          .asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setDoOutput(true)

        val out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream, "UTF-8"))
        out.write(s"""{ "text" : "$data" }""")
        out.flush()
        out.close()
        conn.connect()
        logger.info("Sent slack notification. Status code :" + conn.getResponseCode)
      }

    override def logStepStart(
        step_run_id: String,
        step_name: String,
        props: Map[String, String],
        step_type: String,
        start_time: Long
    ): Task[Unit] = ZIO.unit
    override def logStepEnd(
        step_run_id: String,
        step_name: String,
        props: Map[String, String],
        step_type: String,
        end_time: Long,
        error: Option[Throwable]
    ): Task[Unit] = Task {
      var slackMessageForSteps = ""
      val elapsedTime          = getTimeDifferenceAsString(end_time, getCurrentTimestamp)
      val step_icon            = if (error.isEmpty) "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

      // Update the slackMessageForSteps variable and get the information of step name and its execution time
      slackMessageForSteps = step_icon + "*" + step_name + "*" + " - (" + elapsedTime + ")"

      // Update the slackMessageForSteps variable and get the information of etl steps properties
      val error_message = error.map(msg => f"error -> ${msg.getMessage}").getOrElse("")

      if (error.isEmpty)
        slackMessageForSteps = slackMessageForSteps
      else
        slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + error_message)

      // Concatenate all the messages with finalSlackMessage
      final_step_message = final_step_message.concat(slackMessageForSteps)
    }
    override def logJobStart(job_name: String, args: String, start_time: Long): Task[Unit] = ZIO.unit
    override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): Task[Unit] = Task {
      val execution_date_time = getTimestampAsString(end_time) // Add time difference in this expression

      val data = finalMessageTemplate(
        job_name,
        execution_date_time,
        final_step_message,
        host_url + job_run_id,
        error
      )

      sendSlackNotification(data)
    }
  }

  def live(slack: Option[schema.Slack], jri: String): ULayer[LogEnv] =
    if (slack.isEmpty)
      nolog
    else
      ZLayer.succeed(SlackLogger(jri, slack))
}
