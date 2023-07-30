package etlflow.model

sealed trait Credential
object Credential {
  final case class GCP(serviceAccountPath: String) extends Credential {
    override def toString: String = "****serviceAccountPath****"
  }

  final case class AWS(accessKey: String, secretKey: String) extends Credential {
    override def toString: String = "****accessKey****secretKey****"
  }

  final case class JDBC(url: String, user: String, password: String, driver: String) extends Credential {
    override def toString: String = s"JDBC with url => $url"
  }

  final case class REDIS(hostName: String, password: Option[String] = None, port: Int = 6379) extends Credential {
    override def toString: String = s"REDIS with url $hostName and port $port"
  }

  final case class SMTP(
      port: String,
      host: String,
      user: String,
      password: String,
      transport_protocol: String = "smtp",
      starttls_enable: String = "true",
      smtp_auth: String = "true"
  ) extends Credential {
    override def toString: String = s"SMTP with host  => $host and user => $user"
  }
}
