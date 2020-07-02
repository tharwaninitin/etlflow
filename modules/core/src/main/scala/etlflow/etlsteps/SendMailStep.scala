package etlflow.etlsteps

import etlflow.utils.{MailClientApi, SMTP}
import zio.Task

case class SendMailStep(
      name: String,
      body: String,
      subject: String,
      recipient_list: List[String] = List(""),
      credentials: SMTP
    )
  extends EtlStep[Unit,Unit] {


  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting SendMailStep")
    Task(MailClientApi.sendMail(recipient_list,body,subject,credentials))
    }

  override def getStepProperties(level: String): Map[String, String] = Map("subject" -> subject, "recipient_list" -> recipient_list.mkString(","))
}



