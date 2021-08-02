package etlflow.etlsteps

import etlflow.coretests.TestSuiteHelper
import etlflow.schema.Credential.SMTP
import zio.ZIO
import zio.test.Assertion._
import zio.test._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SendMailStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper with SensorStep  {

  val emailBody: String = {
    val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
    s"""
       | SMTP Email Test
       | Time of Execution: $exec_time
       |""".stripMargin
  }

  val smtp = SMTP(
    sys.env.getOrElse("SMTP_PORT", "587"),
    sys.env.getOrElse("SMTP_HOST", "..."),
    sys.env.getOrElse("SMTP_USER", "..."),
    sys.env.getOrElse("SMTP_PASS", "..."),
  )

  val step = SendMailStep(
    name = "SendSMTPEmail",
    body = emailBody,
    subject = "SendMailStep Test Ran Successfully",
    sender  = Some(sys.env.getOrElse("SMTP_SENDER", "...")),
    recipient_list = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
    credentials = SMTP(
      sys.env.getOrElse("SMTP_PORT", "587"),
      sys.env.getOrElse("SMTP_HOST", "..."),
      sys.env.getOrElse("SMTP_USER", "..."),
      sys.env.getOrElse("SMTP_PASS", "..."),
    )
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("SendMailStepTestSuite") (
      testM("Execute SendMailStep") {
        val step = SendMailStep(
          name = "SendSMTPEmail",
          body = emailBody,
          subject = "SendMailStep Test Ran Successfully",
          sender  = Some(sys.env.getOrElse("SMTP_SENDER", "...")),
          recipient_list = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
          credentials = smtp
        ).process().provideCustomLayer(fullLayer)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val props = step.getStepProperties()
        assert(props)(equalTo(Map("subject" -> "SendMailStep Test Ran Successfully", "recipient_list" -> "swapnil.sonawane@startv.com")))
      }
    )
}
