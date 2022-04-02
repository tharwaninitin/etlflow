package etlflow.etlsteps

import etlflow.email.MailClientApi
import etlflow.model.Credential.SMTP
import zio.Task

case class SendMailStep(
    name: String,
    body: String,
    subject: String,
    recipientList: List[String],
    credentials: SMTP,
    sender: Option[String] = None
) extends EtlStep[Unit] {
  override protected type R = Any

  override protected def process: Task[Unit] = Task {
    logger.info("#" * 100)
    logger.info(s"Starting SendMailStep")
    MailClientApi.sendMail(sender, recipientList, body, subject, credentials)
  }

  override def getStepProperties: Map[String, String] = Map("subject" -> subject, "recipient_list" -> recipientList.mkString(","))
}
