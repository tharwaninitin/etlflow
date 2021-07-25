//package etlflow.coretests.steps.sendMail
//
//import etlflow.coretests.steps.sendMail.SendMailStepTestSuite.{emailBody}
//import etlflow.etlsteps.SendMailStep
//import etlflow.schema.Credential.SMTP
//import etlflow.utils.MailClientApi
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should
//
//class SendMailPropsTestSuite extends AnyFlatSpec with should.Matchers {
//
//
//  val step = SendMailStep(
//    name = "SendSMTPEmail",
//    body = emailBody,
//    subject = "SendMailStep Test Ran Successfully",
//    sender  = Some(sys.env.getOrElse("SMTP_SENDER", "...")),
//    recipient_list = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
//    credentials = SMTP(
//      sys.env.getOrElse("SMTP_PORT", "587"),
//      sys.env.getOrElse("SMTP_HOST", "..."),
//      sys.env.getOrElse("SMTP_USER", "..."),
//      sys.env.getOrElse("SMTP_PASS", "..."),
//    )
//  )
//
//  val props = step.getStepProperties()
//
//  "getStepProperties should  " should "run successfully correct props" in {
//    assert(props ==  Map("subject" -> "SendMailStep Test Ran Successfully", "recipient_list" -> "swapnil.sonawane@startv.com"))
//  }
//}
