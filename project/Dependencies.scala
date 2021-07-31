import sbt._

object Dependencies {
  val ZioVersion = "1.0.10"
  val ZioCatsInteropVersion = "3.1.1.0"
  val ZioHttpVersion = "1.0.0.0-RC17"
  val ZioConfig = "1.0.6"
  val CalibanVersion ="1.1.0"

  val CatsCoreVersion = "2.6.1"
  val CatsEffectVersion = "3.2.1"
  val Cron4sVersion = "0.6.1"
  val Fs2Version = "3.0.6"
  val Fs2BlobStoreVersion = "0.9.3"
  val CirceVersion = "0.14.1"
  val DoobieVersion = "1.0.0-M5"
  val ShapelessVersion = "2.3.7"
  val ScalaCacheVersion = "0.28.0"

  val SttpVersion = "3.3.12"
  val PrettyTimeVersion = "5.0.1.Final"
  val SparkVersion = "2.4.8"
  val SparkBQVersion = "0.21.1"
  val GcpBqVersion = "1.137.1"
  val GcpDpVersion = "1.5.3"
  val GcpGcsVersion = "1.118.0"
  val GcpPubSubVersion = "1.113.3"
  val HadoopGCSVersion = "1.9.4-hadoop3"
  val HadoopS3Version = "3.3.1"
  val AwsS3Version = "2.17.9"

  val FlywayVersion = "7.12.0"
  val Json4sVersion = "3.6.11"
  val ScoptVersion = "4.0.1"
  val LogbackVersion = "1.2.5"
  val PgVersion = "42.2.23"
  val RedisVersion = "3.30"
  val MailVersion = "1.6.2"
  val JwtCoreVersion = "8.0.3"
  val Sl4jVersion = "1.7.32"
  val BcryptVersion = "4.3.0"

  val ScalaTestVersion = "3.2.9"

  lazy val coreLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "dev.zio" %% "zio-config" % ZioConfig,
    "dev.zio" %% "zio-config-magnolia" % ZioConfig,
    "dev.zio" %% "zio-config-typesafe" % ZioConfig,
    "com.github.scopt" %% "scopt" % ScoptVersion,
    "com.github.t3hnar" %% "scala-bcrypt" % BcryptVersion,
    "javax.mail" % "javax.mail-api" % MailVersion,
    "com.sun.mail" % "javax.mail" % MailVersion
  )

  lazy val cloudLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "dev.zio" %% "zio-interop-cats" % ZioCatsInteropVersion,
    "co.fs2" %% "fs2-core" % Fs2Version,
    "co.fs2" %% "fs2-io" % Fs2Version,
    "org.typelevel" %% "cats-core" % CatsCoreVersion,
    "org.typelevel" %% "cats-effect" % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-kernel" % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-std" % CatsEffectVersion,
    "com.github.fs2-blobstore" %% "gcs" % Fs2BlobStoreVersion,
    "com.github.fs2-blobstore" %% "s3" % Fs2BlobStoreVersion,
    "com.google.cloud" % "google-cloud-bigquery" % GcpBqVersion,
    "com.google.cloud" % "google-cloud-dataproc" % GcpDpVersion,
    "com.google.cloud" % "google-cloud-storage" % GcpGcsVersion,
    "software.amazon.awssdk" % "s3" % AwsS3Version
  )

  lazy val dbLibs = List(
    "dev.zio"       %% "zio"                % ZioVersion,
    "dev.zio"       %% "zio-interop-cats"   % ZioCatsInteropVersion,
    "co.fs2"        %% "fs2-core"           % Fs2Version,
    "co.fs2"        %% "fs2-io"             % Fs2Version,
    "org.typelevel" %% "cats-core"          % CatsCoreVersion,
    "org.typelevel" %% "cats-effect"        % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-kernel" % CatsEffectVersion,
    "org.typelevel" %% "cats-effect-std"    % CatsEffectVersion,
    "org.tpolecat"  %% "doobie-core"        % DoobieVersion,
    "org.tpolecat"  %% "doobie-postgres"    % DoobieVersion,
    "org.tpolecat"  %% "doobie-hikari"      % DoobieVersion,
    "org.flywaydb"   % "flyway-core"          % FlywayVersion
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

  lazy val utilsLibs = List(
    "org.slf4j" % "slf4j-api" % Sl4jVersion
  )

  lazy val jsonLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "org.typelevel" %% "cats-core" % CatsCoreVersion
  )

  lazy val coreTestLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
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
    "org.tpolecat" %% "doobie-scalatest" % DoobieVersion,
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
    "dev.zio" %% "zio-test" % ZioVersion,
    "dev.zio" %% "zio-test-sbt" % ZioVersion
  ).map(_ % Test)

  lazy val jsonTestLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
    "dev.zio" %% "zio-test" % ZioVersion,
    "dev.zio" %% "zio-test-sbt" % ZioVersion
  ).map(_ % Test)

  lazy val utilsTestLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)
}
