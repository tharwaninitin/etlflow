package etlflow.model

sealed trait Credential
object Credential {
  final case class GCP(service_account_key_path: String, project_id: String = "") extends Credential {
    override def toString: String = "****service_account_key_path****"
  }

  final case class AWS(access_key: String, secret_key: String) extends Credential {
    override def toString: String = "****access_key****secret_key****"
  }

  final case class JDBC(url: String, user: String, password: String, driver: String) extends Credential {
    override def toString: String = s"JDBC with url => $url"
  }

  final case class REDIS(host_name: String, password: Option[String] = None, port: Int = 6379) extends Credential {
    override def toString: String = s"REDIS with url $host_name and port $port"
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
