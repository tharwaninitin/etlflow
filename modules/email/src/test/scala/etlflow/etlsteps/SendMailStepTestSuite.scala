package etlflow.etlsteps

import etlflow.etltask.SendMailTask
import etlflow.log.noLog
import etlflow.model.Credential.SMTP
import zio.ZIO
import zio.test.Assertion._
import zio.test._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SendMailStepTestSuite extends DefaultRunnableSpec {

  val emailBody: String = {
    val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
    s"""
       | SMTP Email Test
       | Time of Execution: $exec_time
       |""".stripMargin
  }

  private val smtp = SMTP(
    sys.env.getOrElse("SMTP_PORT", "587"),
    sys.env.getOrElse("SMTP_HOST", "..."),
    sys.env.getOrElse("SMTP_USER", "..."),
    sys.env.getOrElse("SMTP_PASS", "...")
  )

  private val step = SendMailTask(
    name = "SendSMTPEmail",
    body = emailBody,
    subject = "SendMailStep Test Ran Successfully",
    sender = Some(sys.env.getOrElse("SMTP_SENDER", "...")),
    recipientList = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
    credentials = SMTP(
      sys.env.getOrElse("SMTP_PORT", "587"),
      sys.env.getOrElse("SMTP_HOST", "..."),
      sys.env.getOrElse("SMTP_USER", "..."),
      sys.env.getOrElse("SMTP_PASS", "...")
    )
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("SendMailStepTestSuite")(
      testM("Execute SendMailStep") {
        val step = SendMailTask(
          name = "SendSMTPEmail",
          body = emailBody,
          subject = "SendMailStep Test Ran Successfully",
          sender = Some(sys.env.getOrElse("SMTP_SENDER", "...")),
          recipientList = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
          credentials = smtp
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val props = step.getStepProperties
        assertTrue(props == Map("subject" -> "SendMailStep Test Ran Successfully", "recipient_list" -> "abcd@abcd.com"))
      }
    ).provideCustomLayerShared(noLog)
}
