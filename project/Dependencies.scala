import sbt._
import Versions._

object Dependencies {

  lazy val coreLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "org.slf4j" % "slf4j-api" % Sl4jVersion
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
    "com.github.fs2-blobstore" %% "s3" % Fs2BlobStoreVersion,
    "com.github.fs2-blobstore" %% "gcs" % Fs2BlobStoreVersion
  )

  lazy val awsLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "software.amazon.awssdk" % "s3" % AwsS3Version
  )

  lazy val gcpLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "com.google.cloud" % "google-cloud-bigquery" % GcpBqVersion,
    "com.google.cloud" % "google-cloud-dataproc" % GcpDpVersion,
    "com.google.cloud" % "google-cloud-storage" % GcpGcsVersion
  )

  lazy val dbLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "org.scalikejdbc" %% "scalikejdbc" % ScalaLikeJdbcVersion,
    "org.postgresql" % "postgresql" % PgVersion,
    "org.flywaydb" % "flyway-core" % FlywayVersion
  )

  lazy val httpLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % SttpVersion,
    "com.softwaremill.sttp.client3" %% "slf4j-backend" % SttpVersion
  )

  lazy val redisLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "net.debasishg" %% "redisclient" % RedisVersion
  )

  lazy val serverLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "dev.zio" %% "zio-config" % ZioConfig,
    "dev.zio" %% "zio-config-typesafe" % ZioConfig,
    "com.github.ghostdogpr" %% "caliban" % CalibanVersion,
    "com.github.ghostdogpr" %% "caliban-zio-http" % CalibanVersion,
    "io.d11" %% "zhttp" % ZioHttpVersion,
    "com.github.jwt-scala" %% "jwt-core" % JwtCoreVersion,
    "org.ocpsoft.prettytime" % "prettytime" % PrettyTimeVersion,
    "com.github.scopt" %% "scopt" % ScoptVersion,
    "com.cronutils" % "cron-utils" % CronUtilsVersion,
  )

  lazy val sparkLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "org.apache.spark" %% "spark-sql" % SparkVersion % Provided
  )

  lazy val jsonLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "org.typelevel" %% "cats-core" % CatsCoreVersion
  )

  lazy val cryptoLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "de.svenkubiak" % "jBCrypt" % BcryptVersion
  )

  lazy val emailLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "javax.mail" % "javax.mail-api" % MailVersion,
    "com.sun.mail" % "javax.mail" % MailVersion
  )

  lazy val cacheLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "com.github.ben-manes.caffeine" % "caffeine" % CaffeineCacheVersion
  )

  lazy val zioTestLibs = List(
    "dev.zio" %% "zio-test" % ZioVersion,
    "dev.zio" %% "zio-test-sbt" % ZioVersion
  ).map(_ % Test)

  lazy val coreTestLibs = List(
    "ch.qos.logback" % "logback-classic" % LogbackVersion
  ).map(_ % Test)

  lazy val dbTestLibs = List(
    "org.postgresql" % "postgresql" % PgVersion
  ).map(_ % Test)

  lazy val cloudTestLibs = List(
    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
    "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
    "org.apache.hadoop" % "hadoop-common" % HadoopS3Version
  ).map(_ % Test)

  lazy val sparkTestLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion,
    //"com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  ).map(_ % Test)

}
