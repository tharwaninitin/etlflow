import sbt._

object Dependencies {
  val ZioVersion = "1.0.6"
  val ZioCatsInteropVersion = "3.1.1.0"
  val CalibanVersion ="1.0.0"

  val CatsCoreVersion = "2.6.0"
  val CatsEffectVersion = "3.1.0"
  val K8sClientVersion = "0.5.0"
  val Cron4sVersion = "0.6.1"
  val Fs2Version = "3.0.2"
  val Fs2PubSubVersion = "0.18.0-M1"
  val Fs2BlobStoreVersion = "0.9.0-beta3"
  val CirceVersion = "0.13.0"
  val CirceConfigVersion = "0.8.0"
  val DoobieVersion = "1.0.0-M4"
  val ShapelessVersion = "2.3.4"
  val SkunkVersion = "0.1.2"
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
  val AwsS3Version = "2.16.70"

  val FlywayVersion = "7.8.1"
  val Json4sVersion = "3.6.11"
  val ScoptVersion = "4.0.1"
  val LogbackVersion = "1.2.3"
  val PgVersion = "42.2.19"
  val RedisVersion = "3.30"
  val mailVersion = "1.6.2"
  val JwtCoreVersion = "7.1.3"
  val Sl4jVersion = "1.7.30"
  val bcryptVersion = "4.3.0"
  val ZioHttpVersion = "0.10.0"

  val ScalaTestVersion = "3.2.8"

  lazy val coreLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "io.circe" %% "circe-optics" % CirceVersion,
    "io.circe" %% "circe-config" % CirceConfigVersion,
    "org.json4s" %% "json4s-jackson" % Json4sVersion,
    "com.github.scopt" %% "scopt" % ScoptVersion,
    "org.slf4j" % "slf4j-api" % Sl4jVersion,
    "com.github.t3hnar" %% "scala-bcrypt" % bcryptVersion,
    "javax.mail" % "javax.mail-api" % mailVersion,
    "com.sun.mail" % "javax.mail"   % mailVersion
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
//    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
//    "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
//    "org.apache.hadoop" % "hadoop-common" % HadoopS3Version
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
    "com.github.ghostdogpr" %% "caliban-zio-http" % ZioHttpVersion,
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
