package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString, getTimestampAsString}
import zio.{UIO, ZIO}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.Var"))
case class Slack(jobRunId: String, slackUrl: String) extends Audit with ApplicationLogger {

  var finalTaskMessage: String = ""
  var finalMessage: String     = ""

  private def finalMessageTemplate(
      jobName: String,
      execDate: String,
      message: String,
      jobRunId: String,
      error: Option[Throwable]
  ): String =
    if (error.isEmpty) {

      finalMessage = finalMessage.concat(f"""
            :large_blue_circle: $jobName *Success!*
            *Time of Execution*: $execDate
            *Job Run ID*: $jobRunId
            *Tasks (Task - Duration)*: $message
            """)
      finalMessage
    } else {

      finalMessage = finalMessage.concat(f"""
            :red_circle: $jobName *Failed!*
            *Time of Execution*: $execDate
            *Job Run ID*: $jobRunId
            *Tasks (Task - Duration)*: $message
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

  override def logTaskStart(
      taskRunId: String,
      taskName: String,
      props: Map[String, String],
      taskType: String,
      startTime: Long
  ): UIO[Unit] = ZIO.unit

  override def logTaskEnd(
      taskRunId: String,
      taskName: String,
      props: Map[String, String],
      taskType: String,
      endTime: Long,
      error: Option[Throwable]
  ): UIO[Unit] = ZIO.succeed {
    var slackMessageForTasks = ""

    val elapsedTime = getTimeDifferenceAsString(endTime, getCurrentTimestamp)

    val taskIcon = if (error.isEmpty) "\n :small_blue_diamond:" else "\n :small_orange_diamond:"

    // Update the slackMessageForTasks variable and get the information of task name and its execution time
    slackMessageForTasks = taskIcon + "*" + taskName + "*" + " - (" + elapsedTime + ")"

    // Update the slackMessageForTasks variable and get the information of etl tasks properties
    val errorMessage = error.map(msg => f"error -> ${msg.getMessage}").getOrElse("")

    if (error.isEmpty)
      slackMessageForTasks = slackMessageForTasks
    else
      slackMessageForTasks = slackMessageForTasks.concat("\n\t\t\t " + errorMessage)

    // Concatenate all the messages with finalSlackMessage
    finalTaskMessage = finalTaskMessage.concat(slackMessageForTasks)
  }

  override def logJobStart(jobName: String, args: Map[String, String], props: Map[String, String], startTime: Long): UIO[Unit] =
    ZIO.unit

  override def logJobEnd(
      jobName: String,
      args: Map[String, String],
      props: Map[String, String],
      endTime: Long,
      error: Option[Throwable]
  ): UIO[Unit] =
    ZIO.fromTry {
      val executionDateTime = getTimestampAsString(endTime) // Add time difference in this expression

      val data = finalMessageTemplate(
        jobName,
        executionDateTime,
        finalTaskMessage,
        jobRunId,
        error
      )

      sendSlackNotification(data)
    }.orDie
}
