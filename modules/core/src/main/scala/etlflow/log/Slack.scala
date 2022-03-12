package etlflow.log

import etlflow.model
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString, getTimestampAsString}
import zio.{Task, UIO, ULayer, ZIO, ZLayer}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import scala.util.Try

object Slack extends ApplicationLogger {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  final case class SlackLogger(jobRunId: String, slack: Option[model.Slack]) extends Service {
    var finalStepMessage: String = ""
    var finalMessage: String     = ""

    val slackEnv: String = slack.map(_.env).getOrElse("")
    val slackUrl: String = slack.map(_.url).getOrElse("")
    val hostUrl: String  = slack.map(_.host).getOrElse("http://localhost:8080/#") + "/JobRunDetails/"

    private def finalMessageTemplate(
        jobName: String,
        execDate: String,
        message: String,
        url: String,
        error: Option[Throwable]
    ): String =
      if (error.isEmpty) {

        finalMessage = finalMessage.concat(f"""
            :large_blue_circle: $slackEnv - $jobName Process *Success!*
            *Time of Execution*: $execDate
            *Details Available at*: $url
            *Steps (Task - Duration)*: $message
            """)
        finalMessage
      } else {

        finalMessage = finalMessage.concat(f"""
            :red_circle: $slackEnv - $jobName Process *Failed!*
            *Time of Execution*: $execDate
            *Details Available at*: $url
            *Steps (Task - Duration)*: $message
            """)
        finalMessage
      }
    private def sendSlackNotification(data: String): Try[Unit] =
      Try {
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val conn = new URL(slackUrl).openConnection().asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setDoOutput(true)

        val out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream, "UTF-8"))
        out.write(s"""{ "text" : "$data" }""")
        out.flush()
        out.close()
        conn.connect()
        logger.info("Sent slack notification. Status code :" + conn.getResponseCode.toString)
      }

    override def logStepStart(
        stepRunId: String,
        stepName: String,
        props: Map[String, String],
        stepType: String,
        startTime: Long
    ): UIO[Unit] = ZIO.unit
    override def logStepEnd(
        stepRunId: String,
        stepName: String,
        props: Map[String, String],
        stepType: String,
        endTime: Long,
        error: Option[Throwable]
    ): UIO[Unit] = UIO {
      var slackMessageForSteps = ""

      val elapsedTime = getTimeDifferenceAsString(endTime, getCurrentTimestamp)

      val stepIcon = if (error.isEmpty) "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

      // Update the slackMessageForSteps variable and get the information of step name and its execution time
      slackMessageForSteps = stepIcon + "*" + stepName + "*" + " - (" + elapsedTime + ")"

      // Update the slackMessageForSteps variable and get the information of etl steps properties
      val errorMessage = error.map(msg => f"error -> ${msg.getMessage}").getOrElse("")

      if (error.isEmpty)
        slackMessageForSteps = slackMessageForSteps
      else
        slackMessageForSteps = slackMessageForSteps.concat("\n\t\t\t " + errorMessage)

      // Concatenate all the messages with finalSlackMessage
      finalStepMessage = finalStepMessage.concat(slackMessageForSteps)
    }
    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] = ZIO.unit
    override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      Task.fromTry {
        val executionDateTime = getTimestampAsString(endTime) // Add time difference in this expression

        val data = finalMessageTemplate(
          jobName,
          executionDateTime,
          finalStepMessage,
          hostUrl + jobRunId,
          error
        )

        sendSlackNotification(data)
      }.orDie
  }

  def live(slack: Option[model.Slack], jri: String): ULayer[LogEnv] =
    if (slack.isEmpty) noLog else ZLayer.succeed(SlackLogger(jri, slack))
}
