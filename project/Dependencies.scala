import sbt._

object Dependencies {
  val ZioVersion = "1.0.5"
  val ZioCatsInteropVersion = "2.3.1.0"
  val CalibanVersion = "0.9.5"

  val CatsCoreVersion = "2.4.2"
  val CatsEffectVersion = "2.3.3"
  val K8sClientVersion = "0.5.0"
  val Cron4sVersion = "0.6.1"
  val Fs2Version = "2.5.3"
  val Fs2PubSubVersion = "0.17.0"
  val Fs2BlobStoreVersion = "0.7.3"
  val CirceVersion = "0.13.0"
  val CirceConfigVersion = "0.8.0"
  val DoobieVersion = "0.12.1"
  val SkunkVersion = "0.0.24"
  val Http4sVersion = "0.21.20"
  val ScalaCacheVersion = "0.28.0"
  val SttpVersion = "3.2.3"
  val TapirVersion = "0.17.19"
  val PrettyTimeVersion = "5.0.0.Final"

  val SparkVersion = "2.4.4"
  val SparkBQVersion = "0.19.1"
  val GcpBqVersion = "1.127.6"
  val GcpDpVersion = "1.2.1"
  val GcpGcsVersion = "1.113.13"
  val GcpPubSubVersion = "1.111.4"
  val HadoopGCSVersion = "1.6.1-hadoop2"
  val HadoopS3Version = "2.10.1"
  val AwsS3Version = "2.16.14"

  val FlywayVersion = "6.4.1"
  val ScoptVersion = "4.0.1"
  val LogbackVersion = "1.2.3"
  val PgVersion = "42.2.19"
  val RedisVersion = "3.30"
  val ScalajHttpVersion = "2.4.2"
  val mailVersion = "1.6.2"
  val JwtCoreVersion = "7.1.3"
  val Sl4jVersion = "1.7.30"
  val bcryptVersion = "4.3.0"

  val ScalaTestVersion = "3.0.5"
  val TestContainerVersion = "1.15.2"

  lazy val zioLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "dev.zio" %% "zio-macros" % ZioVersion,
    "dev.zio" %% "zio-interop-cats" % ZioCatsInteropVersion
  )

  lazy val catsLibs = List(
    "org.typelevel" %% "cats-core" % CatsCoreVersion,
    "org.typelevel" %% "cats-effect" % CatsEffectVersion
  )

  lazy val dbLibs = List(
    "org.tpolecat" %% "doobie-core"     % DoobieVersion,
    "org.tpolecat" %% "doobie-postgres" % DoobieVersion,
    "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
    "org.tpolecat" %% "skunk-core"      % SkunkVersion,
    "org.flywaydb" % "flyway-core"      % FlywayVersion
  )

  lazy val streamingLibs = List(
    "co.fs2" %% "fs2-core" % Fs2Version,
    "co.fs2" %% "fs2-io" % Fs2Version,
    "com.permutive" %% "fs2-google-pubsub-grpc" % Fs2PubSubVersion,
    "com.github.fs2-blobstore" %% "gcs" % Fs2BlobStoreVersion,
    "com.github.fs2-blobstore" %% "s3" % Fs2BlobStoreVersion
  )

  lazy val jsonLibs = List(
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "io.circe" %% "circe-optics" % CirceVersion,
    "io.circe" %% "circe-config" % CirceConfigVersion,
    "org.json4s" %% "json4s-jackson" % "3.5.3"
  )

  lazy val googleCloudLibs = List(
    "com.google.cloud" % "google-cloud-bigquery" % GcpBqVersion,
    "com.google.cloud" % "google-cloud-dataproc" % GcpDpVersion,
    "com.google.cloud" % "google-cloud-storage" % GcpGcsVersion,
    "com.google.cloud" % "google-cloud-pubsub" % GcpPubSubVersion
  )

  lazy val awsLibs = List(
    "software.amazon.awssdk" % "s3" % AwsS3Version
  )

  lazy val sparkLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion
  )

  lazy val redis = List(
    "net.debasishg" %% "redisclient" % RedisVersion
  )

  lazy val httpClient = List(
    "com.softwaremill.sttp.client3" %% "httpclient-backend-zio" % SttpVersion,
    "com.softwaremill.sttp.client3" %% "slf4j-backend" % SttpVersion
  )

  lazy val mail = List(
    "javax.mail" % "javax.mail-api" % mailVersion,
    "com.sun.mail" % "javax.mail"   % mailVersion
  )

  lazy val kubernetes = List(
    "com.goyeau" %% "kubernetes-client" % K8sClientVersion
  )

  lazy val miscLibs = List(
    "com.github.scopt" %% "scopt" % ScoptVersion,
    "org.slf4j" % "slf4j-api" % Sl4jVersion,
    "com.github.t3hnar" %% "scala-bcrypt" % bcryptVersion
  )

  lazy val serverLibs = List(
    "com.github.ghostdogpr" %% "caliban" % CalibanVersion,
    "com.github.ghostdogpr" %% "caliban-http4s" % CalibanVersion,
    "org.http4s" %% "http4s-blaze-client" % Http4sVersion,
    "org.http4s" %% "http4s-blaze-server" % Http4sVersion,
    "org.http4s" %% "http4s-prometheus-metrics" % Http4sVersion,
    "com.github.jwt-scala" %% "jwt-core" % JwtCoreVersion,
    "com.github.alonsodomin.cron4s" %% "cron4s-core" % Cron4sVersion,
    "com.github.cb372" %% "scalacache-caffeine" % ScalaCacheVersion,
    "com.github.cb372" %% "scalacache-cats-effect" % ScalaCacheVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % TapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % TapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-http4s" % TapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % TapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % TapirVersion,
    "org.ocpsoft.prettytime" % "prettytime" % PrettyTimeVersion
    //"com.softwaremill.sttp.tapir" %% "tapir-sttp-client" % TapirVersion,
  )

  lazy val coreTestLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
    "org.testcontainers" % "postgresql" % TestContainerVersion,
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
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  ).map(_ % Test)

  lazy val serverTestLibs = List(
    "org.tpolecat" %% "doobie-scalatest" % DoobieVersion
  ).map(_ % Test)
}
