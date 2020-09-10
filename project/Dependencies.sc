
import mill._
import scalalib._
import scalafmt._

object Dependencies {
  val SparkVersion = "2.4.4"
  val GcpBqVersion = "1.80.0"
  val GcpDpVersion = "0.122.1"
  val GcpGcsVersion = "1.108.0"
  val GcpPubSubVersion = "1.108.1"
  val ScoptVersion = "3.7.1"
  val ZioVersion = "1.0.0"
  val ZioCatsInteropVersion = "2.1.4.0"
  val DoobieVersion = "0.9.0"
  val SkunkVersion = "0.0.18"
  val CalibanVersion = "0.9.1"
  val Http4sVersion = "0.21.3"
  val FlywayVersion = "6.4.1"
  val AwsS3Version = "2.13.23"
  val LogbackVersion = "1.2.3"
  val CirceVersion = "0.13.0"
  val CirceConfigVersion = "0.8.0"
  val ScalaTestVersion = "3.0.5"
  val TestContainerVersion = "1.11.2"
  val SparkBQVersion = "0.16.1"
  val HadoopGCSVersion = "1.6.1-hadoop2"
  val HadoopS3Version = "2.10.0"
  val PgVersion = "42.2.8"

  val redisVesrion = "3.30"
  val scalajHttpVesrion = "2.4.2"
  val mailVersion = "1.6.2"

  lazy val googleCloudLibs = Agg(
    ivy"com.google.cloud:google-cloud-bigquery:$GcpBqVersion",
    ivy"com.google.cloud:google-cloud-dataproc:$GcpDpVersion",
    ivy"com.google.cloud:google-cloud-storage:$GcpGcsVersion",
    ivy"com.google.cloud:google-cloud-pubsub:$GcpPubSubVersion"
  )

  lazy val catsLibs = Agg(
    ivy"org.typelevel::cats-core:2.1.1",
    ivy"org.typelevel::cats-effect:2.1.4"
  )

  lazy val streamingLibs = Agg(
    ivy"co.fs2::fs2-core:2.4.4",
    ivy"co.fs2::fs2-io:2.4.4",
    ivy"org.tpolecat::skunk-core:$SkunkVersion",
    ivy"com.permutive::fs2-google-pubsub-grpc:0.16.0",
    ivy"com.github.fs2-blobstore::gcs:0.7.3",
    ivy"com.github.fs2-blobstore::s3:0.7.3"
  )

  lazy val jsonLibs = Agg(
    ivy"io.circe::circe-core:$CirceVersion",
    ivy"io.circe::circe-generic:$CirceVersion",
    ivy"io.circe::circe-parser:$CirceVersion",
    ivy"io.circe::circe-optics:$CirceVersion",
    ivy"io.circe::circe-config:$CirceConfigVersion",
    ivy"org.json4s::json4s-jackson:3.5.3"
  )

  lazy val awsLibs = Agg(
    ivy"software.amazon.awssdk:s3:$AwsS3Version"
  )

  lazy val sparkLibs = Agg(
    ivy"org.apache.spark::spark-sql:$SparkVersion"
  )

  lazy val redis = Agg(
    ivy"net.debasishg::redisclient:$redisVesrion"
  )

  lazy val scalajHttp = Agg(
    ivy"org.scalaj::scalaj-http:$scalajHttpVesrion"
  )

  lazy val mail = Agg(
    ivy"javax.mail:javax.mail-api:$mailVersion",
    ivy"com.sun.mail:javax.mail:$mailVersion"
  )

  lazy val kubernetes = Agg(
    ivy"com.goyeau::kubernetes-client:35662a1"
  )

  lazy val dbLibs = Agg(
    ivy"org.tpolecat::doobie-core:$DoobieVersion",
    ivy"org.tpolecat::doobie-postgres:$DoobieVersion",
    ivy"org.tpolecat::doobie-hikari:$DoobieVersion",
    ivy"org.tpolecat::doobie-quill:$DoobieVersion",
    ivy"org.flywaydb:flyway-core:$FlywayVersion"
  )

  lazy val zioLibs = Agg(
    ivy"dev.zio::zio:$ZioVersion",
    ivy"dev.zio::zio-interop-cats:$ZioCatsInteropVersion"
  )

  lazy val miscLibs = Agg(
    ivy"com.github.scopt::scopt:$ScoptVersion",
    ivy"org.slf4j:slf4j-api:1.7.30"
    //    "com.github.pureconfig::pureconfig:0.13.0"
  )

  lazy val caliban = Agg(
    ivy"com.github.ghostdogpr::caliban:$CalibanVersion",
    ivy"com.github.ghostdogpr::caliban-http4s:$CalibanVersion",
    ivy"org.http4s::http4s-prometheus-metrics:$Http4sVersion",
    ivy"eu.timepit::fs2-cron-core:0.2.2",
    ivy"com.github.cb372::scalacache-caffeine:0.28.0"
  )

  lazy val http4sclient = Agg(
    ivy"org.http4s::http4s-blaze-client:$Http4sVersion"
  )

  lazy val jwt = Agg(
    ivy"com.pauldijou::jwt-core:4.2.0"
  )

  lazy val coreTestLibs = Agg(
    ivy"org.scalatest::scalatest:$ScalaTestVersion",
    ivy"org.testcontainers:postgresql:$TestContainerVersion",
    ivy"dev.zio::zio-test:$ZioVersion",
    ivy"dev.zio::zio-test-sbt:$ZioVersion",
    ivy"ch.qos.logback:logback-classic:$LogbackVersion",
    ivy"org.postgresql:postgresql:$PgVersion"
  )

  lazy val cloudTestLibs = Agg(
    ivy"org.scalatest::scalatest:$ScalaTestVersion",
    ivy"org.testcontainers:postgresql:$TestContainerVersion",
    ivy"dev.zio::zio-test:$ZioVersion",
    ivy"dev.zio::zio-test-sbt:$ZioVersion",
    ivy"ch.qos.logback:logback-classic:$LogbackVersion",
    ivy"org.postgresql:postgresql:$PgVersion",
    ivy"com.google.cloud.spark::spark-bigquery-with-dependencies:$SparkBQVersion",
    ivy"com.google.cloud.bigdataoss:gcs-connector:$HadoopGCSVersion",
    ivy"org.apache.hadoop:hadoop-aws:$HadoopS3Version",
    ivy"org.apache.hadoop:hadoop-common:$HadoopS3Version"
  )
}
