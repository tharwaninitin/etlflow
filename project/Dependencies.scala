import sbt._

object Dependencies {
  val ZioVersion = "1.0.9"
  val ZioCatsInteropVersion = "3.1.1.0"
  val CalibanVersion ="1.0.1"

  val CatsCoreVersion = "2.6.1"
  val CatsEffectVersion = "3.1.1"
  val Cron4sVersion = "0.6.1"
  val Fs2Version = "3.0.4"
  val Fs2PubSubVersion = "0.18.0-M1"
  val CirceVersion = "0.14.1"
  val CirceConfigVersion = "0.8.0"
  val DoobieVersion = "1.0.0-M5"
  val ShapelessVersion = "2.3.7"
  val SkunkVersion = "0.2.0"
  val ScalaCacheVersion = "0.28.0"
  val SttpVersion = "3.3.7"
  val PrettyTimeVersion = "5.0.1.Final"

  val SparkVersion = "2.4.4"
  val SparkBQVersion = "0.21.0"
  val GcpBqVersion = "1.133.1"
  val GcpDpVersion = "1.5.2"
  val GcpGcsVersion = "1.116.0"
  val GcpPubSubVersion = "1.113.3"
  val HadoopGCSVersion = "1.9.4-hadoop3"
  val HadoopS3Version = "3.3.1"
  val AwsS3Version = "2.16.87"

  val FlywayVersion = "7.10.0"
  val Json4sVersion = "3.6.11"
  val ScoptVersion = "4.0.1"
  val LogbackVersion = "1.2.3"
  val PgVersion = "42.2.22"
  val RedisVersion = "3.30"
  val MailVersion = "1.6.2"
  val JwtCoreVersion = "8.0.2"
  val Sl4jVersion = "1.7.31"
  val BcryptVersion = "4.3.0"

  val ZioHttpVersion = "1.0.0.0-RC17"
  val ScalaTestVersion = "3.2.9"

  lazy val coreLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "io.circe" %% "circe-optics" % CirceVersion,
    "io.circe" %% "circe-config" % CirceConfigVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "org.json4s" %% "json4s-jackson" % Json4sVersion,
    "com.github.scopt" %% "scopt" % ScoptVersion,
    "org.slf4j" % "slf4j-api" % Sl4jVersion,
    "com.github.t3hnar" %% "scala-bcrypt" % BcryptVersion,
    "javax.mail" % "javax.mail-api" % MailVersion,
    "com.sun.mail" % "javax.mail"   % MailVersion
  )

  lazy val cloudLibs = List(
    "org.tpolecat" %% "skunk-core" % SkunkVersion,
    "com.permutive" %% "fs2-google-pubsub-grpc" % Fs2PubSubVersion,
    "co.fs2"        %% "fs2-reactive-streams"      % Fs2Version,
    "com.google.cloud" % "google-cloud-bigquery" % GcpBqVersion,
    "com.google.cloud" % "google-cloud-dataproc" % GcpDpVersion,
    "com.google.cloud" % "google-cloud-storage" % GcpGcsVersion,
    "com.google.cloud" % "google-cloud-pubsub" % GcpPubSubVersion,
    "software.amazon.awssdk" % "s3" % AwsS3Version
  )

  lazy val dbLibs = List(
    "dev.zio"      %% "zio" % ZioVersion,
    "dev.zio"      %% "zio-interop-cats" % ZioCatsInteropVersion,
    "co.fs2"       %% "fs2-core"         % Fs2Version,
    "co.fs2"       %% "fs2-io"           % Fs2Version,
    "com.chuusai"  %% "shapeless"        % ShapelessVersion,
    "org.tpolecat" %% "doobie-core"     % DoobieVersion,
    "org.tpolecat" %% "doobie-postgres" % DoobieVersion,
    "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
    "org.flywaydb" % "flyway-core"      % FlywayVersion,
    "com.github.alonsodomin.cron4s" %% "cron4s-core" % Cron4sVersion,
    "org.typelevel" %% "cats-core" % CatsCoreVersion,
    "org.typelevel" %% "cats-effect" % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-kernel" % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-std"    % CatsEffectVersion
  )

  lazy val httpLibs = List(
    "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % SttpVersion,
    "com.softwaremill.sttp.client3" %% "slf4j-backend" % SttpVersion
  )

  lazy val redisLibs = List(
    "net.debasishg" %% "redisclient" % RedisVersion
  )

  lazy val serverLibs = List(
    "com.github.ghostdogpr" %% "caliban" % CalibanVersion,
    "com.github.ghostdogpr" %% "caliban-zio-http" % CalibanVersion,
    "io.d11" %% "zhttp" % ZioHttpVersion,
    "com.github.jwt-scala" %% "jwt-core" % JwtCoreVersion,
    "com.github.alonsodomin.cron4s" %% "cron4s-core" % Cron4sVersion,
    "com.github.cb372" %% "scalacache-caffeine" % ScalaCacheVersion,
    "org.ocpsoft.prettytime" % "prettytime" % PrettyTimeVersion
  )

  lazy val sparkLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion % Provided
  )

  lazy val coreTestLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
    "org.tpolecat" %% "doobie-scalatest" % DoobieVersion,
    "dev.zio" %% "zio-test" % ZioVersion,
    "dev.zio" %% "zio-test-sbt" % ZioVersion,
    "ch.qos.logback" % "logback-classic" % LogbackVersion,
    "org.postgresql" % "postgresql" % PgVersion
  ).map(_ % Test)

  lazy val cloudTestLibs = List(
    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
    "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
    "org.apache.hadoop" % "hadoop-common" % HadoopS3Version
  ).map(_ % Test)

  lazy val sparkTestLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion,
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  ).map(_ % Test)

  lazy val dbTestLibs = List(
    "io.circe" %% "circe-config" % CirceConfigVersion,
    "io.circe" %% "circe-generic" % CirceVersion
  ).map(_ % Test)
}
