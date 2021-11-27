package etlflow.utils

import etlflow.schema.{Config, WebServer, _}
import zio.{IO, ZIO}
import zio.config.typesafe.TypesafeConfigSource
import etlflow.schema.Credential.JDBC
import zio.config._
import ConfigDescriptor._

object Configuration {

  val db: ConfigDescriptor[JDBC] = (
    string("url") |@|
      string("user") |@|
      string("password") |@|
      string("driver")
    )(JDBC.apply, b => Some(b.url, b.user, b.password, b.driver))

  val dataprocSpark: ConfigDescriptor[DataprocSpark] = (
    string("mainclass") |@|
      list("deplibs")(string)
    )(DataprocSpark.apply, b => Some(b.mainclass, b.deplibs))

  val slack: ConfigDescriptor[Slack] = (
    string("url") |@|
      string("env") |@|
      string("host")
    )(Slack.apply, b => Some(b.url, b.env, b.host))

  val webServer: ConfigDescriptor[WebServer] = (
    string("ip_address").optional |@|
      int("port").optional |@|
      set("allowedOrigins")(string).optional
    )(WebServer.apply, b => Some(b.ip_address, b.port, b.allowedOrigins))

  val applicationConf: ConfigDescriptor[Config]  = (
    nested("db")(db).optional |@|
      string("timezone").optional |@|
      nested("slack")(slack).optional |@|
      nested("dataproc")(dataprocSpark).optional |@|
      list("token")(string).optional |@|
      nested("webserver")(webServer).optional |@|
      string("secretkey").optional
    )(Config.apply, b => Some(b.db, b.timezone, b.slack, b.dataproc, b.token, b.webserver, b.secretkey))

  lazy val config: IO[ReadError[String], Config] =
    TypesafeConfigSource.fromDefaultLoader
      .flatMap(source => ZIO.fromEither(read(applicationConf from source)))
}
