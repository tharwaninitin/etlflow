package etlflow.etlsteps

import etlflow.email.MailClientApi
import etlflow.model.Credential.SMTP
import zio.Task

case class SendMailStep(
    name: String,
    body: String,
    subject: String,
    sender: Option[String] = None,
    recipient_list: List[String] = List(""),
    credentials: SMTP
) extends EtlStep[Any, Unit] {

  final def process: Task[Unit] = Task {
    logger.info("#" * 100)
    logger.info(s"Starting SendMailStep")
    MailClientApi.sendMail(sender, recipient_list, body, subject, credentials)
  }

  override def getStepProperties: Map[String, String] =
    Map("subject" -> subject, "recipient_list" -> recipient_list.mkString(","))
}
