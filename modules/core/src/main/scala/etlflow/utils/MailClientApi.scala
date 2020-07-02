package etlflow.utils

import java.util.Properties

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Session}
import org.slf4j.{Logger, LoggerFactory}

object MailClientApi {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def sendMail(recipient: List[String],template:String,subject:String,credentials: SMTP): Option[Int] = {
    try {
      val content = template
      val properties = new Properties

      properties.put("mail.smtp.port", credentials.port)
      properties.setProperty("mail.transport.protocol", credentials.transport_protocol)
      properties.setProperty("mail.smtp.starttls.enable", credentials.starttls_enable)
      properties.setProperty("mail.host", credentials.host)
      properties.setProperty("mail.user", credentials.user)
      properties.setProperty("mail.password", credentials.password)
      properties.setProperty("mail.smtp.auth", credentials.smtp_auth)

      val session = Session.getDefaultInstance(properties)

      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(credentials.user))
      val recipientAddress: Array[Address] = (recipient map { recipient => new InternetAddress(recipient) }).toArray
      message.addRecipients(Message.RecipientType.TO, recipientAddress)
      message.setSubject(subject)
      message.setHeader("Content-Type", "text/plain;")
      message.setContent(content, "text/html")
      val transport = session.getTransport(credentials.transport_protocol)
      transport.connect(credentials.host, credentials.user, credentials.password)
      transport.sendMessage(message, message.getAllRecipients)
      logger.info("Email Sent!!")
      Some(recipient.size)
    }
    catch {
      case exception: Exception =>
        logger.info("Mail delivery failed. " + exception)
        None
    }
  }
}
