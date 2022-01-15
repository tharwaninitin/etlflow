package etlflow

package object model {
  case class Config(
      db: Option[Credential.JDBC],
      timezone: Option[String],
      slack: Option[Slack],
      dataproc: Option[DataprocSpark],
      token: Option[List[String]],
      webserver: Option[WebServer],
      secretkey: Option[String]
  )
  case class DataprocSpark(mainclass: String, deplibs: List[String])
  case class Slack(url: String, env: String, host: String)
  case class WebServer(ip_address: Option[String], port: Option[Int], allowedOrigins: Option[Set[String]])
}
