import sbt._

object Dependencies {
  val SparkVersion = "2.4.4"
  val GcpBqVersion = "1.80.0"
  val GcpDpVersion = "0.122.1"
  val GcpGcsVersion = "1.108.0"
  val ScoptVersion = "3.7.1"
  val ZioVersion = "1.0.0-RC18-2"
  val ZioCatsInteropVersion = "2.0.0.0-RC11"
  val DoobieVersion = "0.8.8"
  val CalibanVersion = "0.7.5"
  val Http4sVersion = "0.21.3"
  val FlywayVersion = "6.4.1"
  val AwsS3Version = "2.13.23"
  val LogbackVersion = "1.2.3"

  val ScalaTestVersion = "3.0.5"
  val TestContainerVersion = "1.11.2"
  val SparkBQVersion = "0.16.1"
  val HadoopGCSVersion = "1.6.1-hadoop2"
  val HadoopS3Version = "2.10.0"
  val PgVersion = "42.2.8"

  val redisVesrion = "3.30"
  val scalajHttpVesrion = "2.4.2"
  val mailVersion = "1.6.2"

  lazy val googleCloudLibs = List(
    "com.google.cloud" % "google-cloud-bigquery" % GcpBqVersion,
    "com.google.cloud" % "google-cloud-dataproc" % GcpDpVersion,
    "com.google.cloud" % "google-cloud-storage" % GcpGcsVersion
  )

  lazy val awsLibs = List(
    "software.amazon.awssdk" % "s3" % AwsS3Version
  )

  lazy val sparkLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion
  )

  lazy val redis = List(
    "net.debasishg" %% "redisclient" % redisVesrion
  )

  lazy val scalajHttp = List(
    "org.scalaj" %% "scalaj-http" % scalajHttpVesrion
  )

  lazy val mail = List(
    "javax.mail" % "javax.mail-api" % mailVersion,
    "com.sun.mail" % "javax.mail"   % mailVersion

  )

  lazy val dbLibs = List(
    "org.tpolecat" %% "doobie-core"     % DoobieVersion,
    "org.tpolecat" %% "doobie-postgres" % DoobieVersion,
    "org.tpolecat" %% "doobie-h2"       % DoobieVersion,
    "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
    "org.tpolecat" %% "doobie-quill"    % DoobieVersion,
    "org.flywaydb" % "flyway-core"      % FlywayVersion
  )

  lazy val zioLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "dev.zio" %% "zio-interop-cats" % ZioCatsInteropVersion
  )

  lazy val miscLibs = List(
    "com.github.scopt" %% "scopt" % ScoptVersion
  )

  lazy val caliban = List(
    "com.github.ghostdogpr" %% "caliban" % CalibanVersion,
    "com.github.ghostdogpr" %% "caliban-http4s" % CalibanVersion,
    "org.http4s" %% "http4s-prometheus-metrics" % Http4sVersion,
    "eu.timepit" %% "fs2-cron-core" % "0.2.2",
    "com.github.cb372" %% "scalacache-caffeine" % "0.28.0"
  )

  lazy val http4sclient = List(
    "org.http4s" %% "http4s-blaze-client" % Http4sVersion
  )

  lazy val jwt = List(
    "com.pauldijou" %% "jwt-core" % "4.2.0"
  )

  lazy val testLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
    "org.testcontainers" % "postgresql" % TestContainerVersion,
    "dev.zio" %% "zio-test"     % ZioVersion,
    "dev.zio" %% "zio-test-sbt" % ZioVersion,
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion,
    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
    "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
    "org.apache.hadoop" % "hadoop-common" % HadoopS3Version,
    "ch.qos.logback" % "logback-classic" % LogbackVersion,
    "org.postgresql" % "postgresql" % PgVersion
  ).map(_ % Test)
}
