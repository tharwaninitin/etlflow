package etlflow.etlsteps

import etlflow.utils.{MailClientApi, SMTP}
import zio.Task
import etlflow.utils.{HttpClientApi, JsonJackson, LoggingLevel}

case class SendMailStep(
                         name: String,
                         body: String,
                         subject: String,
                         recipient_list: List[String] = List(""),
                         credentials: SMTP
                       )
  extends EtlStep[Unit,Unit] {


  final def process(in: =>Unit): Task[Unit] = Task {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting SendMailStep")
    MailClientApi.sendMail(recipient_list,body,subject,credentials)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("subject" -> subject, "recipient_list" -> recipient_list.mkString(","))
}



