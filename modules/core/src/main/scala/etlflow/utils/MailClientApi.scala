package etlflow.utils

import java.util.Properties
import etlflow.Credential.SMTP
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Session}
import org.slf4j.{Logger, LoggerFactory}

object MailClientApi {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def sendMail(recipient: List[String],content:String,subject:String,credentials: SMTP): Unit = {
    try {
      val properties = new Properties
      properties.put("mail.smtp.port", credentials.port)
      properties.setProperty("mail.transport.protocol", credentials.transport_protocol)
      properties.setProperty("mail.smtp.starttls.enable", credentials.starttls_enable)
      properties.setProperty("mail.host", credentials.host)
      properties.setProperty("mail.user", credentials.user)
      properties.setProperty("mail.password", credentials.password)
      properties.setProperty("mail.smtp.auth", credentials.smtp_auth)

      val session = Session.getDefaultInstance(properties)
      val recipientAddress: Array[Address] = (recipient map { recipient => new InternetAddress(recipient) }).toArray

      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(credentials.user))
      message.addRecipients(Message.RecipientType.TO, recipientAddress)
      message.setSubject(subject)
      message.setHeader("Content-Type", "text/plain;")
      message.setContent(content, "text/html")

      val transport = session.getTransport(credentials.transport_protocol)
      transport.connect(credentials.host, credentials.user, credentials.password)
      transport.sendMessage(message, message.getAllRecipients)
      logger.info("Email Sent!!")
      logger.info("#"*100)
    }
    catch {
      case exception: Exception =>
        logger.error("Mail delivery failed. " + exception)
        throw exception
    }
  }
}
