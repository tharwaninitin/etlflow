package etlflow.etlsteps

import etlflow.schema.Credential.SMTP
import etlflow.schema.LoggingLevel
import etlflow.utils.MailClientApi
import zio.Task

case class SendMailStep(
                         name: String,
                         body: String,
                         subject: String,
                         sender: Option[String]= None,
                         recipient_list: List[String] = List(""),
                         credentials: SMTP
                       )
  extends EtlStep[Unit,Unit] {


  final def process(in: =>Unit): Task[Unit] = Task {
    logger.info("#"*100)
    logger.info(s"Starting SendMailStep")
    MailClientApi.sendMail(sender,recipient_list,body,subject,credentials)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("subject" -> subject, "recipient_list" -> recipient_list.mkString(","))
}



