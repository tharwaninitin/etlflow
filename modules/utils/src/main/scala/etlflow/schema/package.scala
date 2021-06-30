package etlflow

package object schema {

  case class Config(db: Credential.JDBC, timezone: Option[String], slack: Option[Slack], dataproc: Option[DataprocSpark], token: Option[List[String]],webserver: Option[WebServer], host: Option[String])
  case class DataprocSpark(mainclass: String, deplibs: List[String])
  case class Slack(url: String, env: String)
  case class WebServer(ip_address:Option[String],port:Option[Int],secretKey:Option[String],allowedOrigins:Option[Set[String]])
}
