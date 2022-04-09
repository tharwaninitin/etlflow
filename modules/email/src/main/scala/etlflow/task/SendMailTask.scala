package etlflow.task

import etlflow.email.MailClientApi
import etlflow.model.Credential.SMTP
import zio.{Task, ZIO}
import scala.util.Try

case class SendMailTask(
    name: String,
    body: String,
    subject: String,
    recipientList: List[String],
    credentials: SMTP,
    sender: Option[String] = None
) extends EtlTask[Any, Unit] {

  override protected def process: Task[Unit] = ZIO.fromTry(processTry)

  override protected def processTry: Try[Unit] = Try {
    logger.info("#" * 100)
    logger.info(s"Starting SendMailTask")
    MailClientApi.sendMail(sender, recipientList, body, subject, credentials)
  }

  override def getTaskProperties: Map[String, String] = Map("subject" -> subject, "recipient_list" -> recipientList.mkString(","))
}
