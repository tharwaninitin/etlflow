package etlflow

package object model {
  case class Config(
      db: Option[Credential.JDBC] = None,
      timezone: Option[String] = None,
      slack: Option[Slack] = None,
      token: Option[List[String]] = None,
      webserver: Option[WebServer] = None,
      secretkey: Option[String] = None
  )
  case class Slack(url: String, env: String, host: String)
  case class WebServer(ipAddress: Option[String], port: Option[Int], allowedOrigins: Option[Set[String]])
}
