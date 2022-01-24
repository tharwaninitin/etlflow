package etlflow.utils

import etlflow.model._
import zio.{IO, ZIO}
import zio.config.typesafe.TypesafeConfigSource
import etlflow.model.Credential.JDBC
import zio.config._
import ConfigDescriptor._

object Configuration {

  val db: ConfigDescriptor[JDBC] = (
    string("url") |@|
      string("user") |@|
      string("password") |@|
      string("driver")
  )(JDBC.apply, JDBC.unapply)

  val dataprocSpark: ConfigDescriptor[DataprocSpark] = (
    string("mainclass") |@|
      list("deplibs")(string)
  )(DataprocSpark.apply, DataprocSpark.unapply)

  val slack: ConfigDescriptor[Slack] = (
    string("url") |@|
      string("env") |@|
      string("host")
  )(Slack.apply, Slack.unapply)

  val webServer: ConfigDescriptor[WebServer] = (
    string("ip_address").optional |@|
      int("port").optional |@|
      set("allowedOrigins")(string).optional
  )(WebServer.apply, WebServer.unapply)

  val applicationConf: ConfigDescriptor[Config] = (
    nested("db")(db).optional |@|
      string("timezone").optional |@|
      nested("slack")(slack).optional |@|
      nested("dataproc")(dataprocSpark).optional |@|
      list("token")(string).optional |@|
      nested("webserver")(webServer).optional |@|
      string("secretkey").optional
  )(Config.apply, Config.unapply)

  lazy val config: IO[ReadError[String], Config] =
    TypesafeConfigSource.fromDefaultLoader
      .flatMap(source => ZIO.fromEither(read(applicationConf.from(source))))
}
