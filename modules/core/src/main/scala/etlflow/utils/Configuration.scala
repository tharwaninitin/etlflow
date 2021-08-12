package etlflow.utils

import etlflow.schema.Config
import zio.{IO, ZIO}
import zio.config.typesafe.TypesafeConfigSource
import zio.config._
import ConfigDescriptor._
import etlflow.schema.Credential.JDBC
import etlflow.schema._

object Configuration {

  val db: ConfigDescriptor[JDBC] = (string("url") |@| string("user") |@| string("password") |@| string("driver"))(JDBC.apply, JDBC.unapply)
  val dataprocSpark = (string("mainclass") |@| list("deplibs")(string))(DataprocSpark.apply, DataprocSpark.unapply)
  val slack = (string("url") |@| string("env") |@| string("host"))(Slack.apply, Slack.unapply)
  val webServer = (string("ip_address").optional |@| int("port").optional |@| string("secretKey").optional
    |@| set("allowedOrigins")(string).optional )(WebServer.apply, WebServer.unapply)


  val applicationConf =
    (nested("db")(db) |@|
      string("timezone").optional |@|
      nested("slack")(slack).optional |@|
      nested("dataproc")(dataprocSpark).optional |@|
      list("token")(string).optional |@|
      nested("webserver")(webServer).optional
      )(Config.apply, Config.unapply)

  lazy val config: IO[ReadError[String], Config] =
    TypesafeConfigSource.fromDefaultLoader
      .flatMap(source => ZIO.fromEither(read(applicationConf from source)))
}
