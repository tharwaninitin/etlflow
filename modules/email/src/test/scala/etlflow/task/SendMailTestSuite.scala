package etlflow.task

import etlflow.log.noLog
import etlflow.model.Credential.SMTP
import zio.ZIO
import zio.test.Assertion._
import zio.test._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SendMailTestSuite extends DefaultRunnableSpec {

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

  private val task = SendMailTask(
    name = "SendSMTPEmail",
    body = emailBody,
    subject = "SendMailTask Test Ran Successfully",
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
    suite("SendMailTaskTestSuite")(
      testM("Execute SendMailTask") {
        val task = SendMailTask(
          name = "SendSMTPEmail",
          body = emailBody,
          subject = "SendMailTask Test Ran Successfully",
          sender = Some(sys.env.getOrElse("SMTP_SENDER", "...")),
          recipientList = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
          credentials = smtp
        ).execute
        assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getTaskProperties") {
        val props = task.getTaskProperties
        assertTrue(props == Map("subject" -> "SendMailTask Test Ran Successfully", "recipient_list" -> "abcd@abcd.com"))
      }
    ).provideCustomLayerShared(noLog)
}
