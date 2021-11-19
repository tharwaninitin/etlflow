package etlflow.log

import etlflow.etlsteps.EtlStep
import etlflow.log.SlackApi.Service
import etlflow.schema.LoggingLevel.{DEBUG, INFO, JOB}
import etlflow.schema.{LoggingLevel, Slack}
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString, getTimestampAsString}
import zio.{Task, UIO, ULayer, ZLayer}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import scala.util.Try

object SlackImplementation extends ApplicationLogger {

  val nolog: ULayer[SlackEnv] = ZLayer.succeed(
    new Service {
      override def getSlackNotification: UIO[String] = UIO("")
      override def logStepEnd(start_time: Long, job_notification_level: LoggingLevel, etlstep: EtlStep[_, _], error_message: Option[String]): Task[Unit] = Task.unit
      override def logJobEnd(job_name: String, job_run_id: String, job_notification_level: LoggingLevel, start_time: Long, error_message: Option[String]): Task[Unit] = Task.unit
    }
  )

  def live(slack: Option[Slack]): ULayer[SlackEnv] = {
    if (slack.isEmpty)
      nolog
    else
      ZLayer.succeed(
        new Service {

          var final_step_message: String = ""
          var final_message: String = ""
          val slack_env: String = slack.map(_.env).getOrElse("")
          val slack_url: String = slack.map(_.url).getOrElse("")
          val host_url: String = slack.map(_.host).getOrElse("http://localhost:8080/#") + "/JobRunDetails/"

          private def finalMessageTemplate(job_name: String, job_notification_level: LoggingLevel, exec_date: String, message: String, url: String, error_message: Option[String]): String = {
            if (error_message.isEmpty) {
              /** Template for slack success message */
              job_notification_level match {
                case JOB =>
                  final_message = final_message.concat(
                    f"""
            :large_blue_circle: $slack_env - $job_name Process *Success!*
            *Time of Execution*: $exec_date
            *Details Available at*: $url
            """)
                  final_message
                case INFO | DEBUG =>
                  final_message = final_message.concat(
                    f"""
            :large_blue_circle: $slack_env - $job_name Process *Success!*
            *Time of Execution*: $exec_date
            *Details Available at*: $url
            *Steps (Task - Duration)*: $message
            """)
                  final_message
              }
            }
            else {
              /** Template for slack failure message * */
              final_message = final_message.concat(
                f"""
            :red_circle: $slack_env - $job_name Process *Failed!*
            *Time of Execution*: $exec_date
            *Details Available at*: $url
            *Steps (Task - Duration)*: $message
            """)
              final_message
            }
          }

          private def sendSlackNotification(data: String): Unit = {
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
          }

          override def getSlackNotification: UIO[String] = UIO(final_message)

          override def logStepEnd(start_time: Long, job_notification_level: LoggingLevel, etlstep: EtlStep[_, _], error_message: Option[String]): Task[Unit] = Task {
            var slackMessageForSteps = ""
            val elapsedTime = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
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
                else if (error.isEmpty && job_notification_level == JOB)
                  slackMessageForSteps = ""
                else
                  slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + error)
            }
            // Concatenate all the messages with finalSlackMessage
            final_step_message = final_step_message.concat(slackMessageForSteps)
          }

          override def logJobEnd(job_name: String, job_run_id: String, job_notification_level: LoggingLevel, start_time: Long, error_message: Option[String]): Task[Unit] = Task {
            val execution_date_time = getTimestampAsString(start_time) // Add time difference in this expression

            val data = finalMessageTemplate(
              job_name,
              job_notification_level,
              execution_date_time,
              final_step_message,
              host_url + job_run_id,
              error_message
            )

            sendSlackNotification(data)
          }
        }
      )
  }
}
