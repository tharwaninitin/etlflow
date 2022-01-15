package etlflow.email

import etlflow.model.Credential.SMTP
import etlflow.utils.{ApplicationLogger, LogTry}
import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Session}

private[etlflow] object MailClientApi extends ApplicationLogger {

  def sendMail(sender: Option[String], recipient: List[String], content: String, subject: String, credentials: SMTP): Unit =
    LogTry {
      val properties = new Properties
      properties.put("mail.smtp.port", credentials.port)
      properties.setProperty("mail.transport.protocol", credentials.transport_protocol)
      properties.setProperty("mail.smtp.starttls.enable", credentials.starttls_enable)
      properties.setProperty("mail.host", credentials.host)
      properties.setProperty("mail.user", credentials.user)
      properties.setProperty("mail.password", credentials.password)
      properties.setProperty("mail.smtp.auth", credentials.smtp_auth)

      val session = Session.getDefaultInstance(properties)

      val recipientAddress: Array[Address] = (recipient.map { recipient => new InternetAddress(recipient) }).toArray

      val message = new MimeMessage(session)
      message.setFrom(sender.getOrElse(new InternetAddress(credentials.user)).toString)
      message.addRecipients(Message.RecipientType.TO, recipientAddress)
      message.setSubject(subject)
      message.setHeader("Content-Type", "text/plain;")
      message.setContent(content, "text/html")

      val transport = session.getTransport(credentials.transport_protocol)
      transport.connect(credentials.host, credentials.user, credentials.password)
      transport.sendMessage(message, message.getAllRecipients)
      logger.info("Email Sent!!")
      logger.info("#" * 100)
    }.get
}
