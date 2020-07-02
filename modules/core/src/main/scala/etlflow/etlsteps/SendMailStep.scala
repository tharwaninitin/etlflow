package etlflow.etlsteps

import etlflow.utils.{MailClientApi, SMTP}
import zio.Task

class SendMailStep(
                    val name: String,
                    val body:String,
                    val subject:String,
                    val recipientList :List[String] = List(""),
                    val credentials: SMTP
                  )
  extends EtlStep[Unit,Unit] {


  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting SendMailStep")
    Task(MailClientApi.sendMail(recipientList,body,subject,credentials))
    }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> name)
}

object SendMailStep {
  def apply(name: String,body:String,subject:String,recipientList:List[String],credentials:SMTP): SendMailStep =
    new SendMailStep(name,body,subject,recipientList,credentials)
}



