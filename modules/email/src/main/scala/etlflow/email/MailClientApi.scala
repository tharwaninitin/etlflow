package etlflow.email

import etlflow.model.Credential.SMTP
import etlflow.utils.{ApplicationLogger, LoggedTry}
import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Session}
import scala.util.{Failure, Success}

private[etlflow] object MailClientApi extends ApplicationLogger {

  @SuppressWarnings(Array("org.wartremover.warts.ToString", "org.wartremover.warts.NonUnitStatements"))
  def sendMail(sender: Option[String], recipient: List[String], content: String, subject: String, credentials: SMTP): Unit =
    LoggedTry {
      val properties = new Properties
      properties.put("mail.smtp.port", credentials.port)
      properties.setProperty("mail.transport.protocol", credentials.transport_protocol)
      properties.setProperty("mail.smtp.starttls.enable", credentials.starttls_enable)
      properties.setProperty("mail.host", credentials.host)
      properties.setProperty("mail.user", credentials.user)
      properties.setProperty("mail.password", credentials.password)
      properties.setProperty("mail.smtp.auth", credentials.smtp_auth)

      val session = Session.getDefaultInstance(properties)

      val recipientAddress: Array[Address] = recipient.map(recipient => new InternetAddress(recipient)).toArray

      val message = new MimeMessage(session)
      sender.fold(message.setFrom(new InternetAddress(credentials.user)))(s => message.setFrom(s))
      message.addRecipients(Message.RecipientType.TO, recipientAddress)
      message.setSubject(subject)
      message.setHeader("Content-Type", "text/plain;")
      message.setContent(content, "text/html")

      val transport = session.getTransport(credentials.transport_protocol)
      transport.connect(credentials.host, credentials.user, credentials.password)
      transport.sendMessage(message, message.getAllRecipients)
    } match {
      case Failure(ex) =>
        logger.error(s"Error in sending Email ${ex.getMessage}")
        logger.info("#" * 100)
      case Success(_) =>
        logger.info("Email Sent!!")
        logger.info("#" * 100)
    }
}
