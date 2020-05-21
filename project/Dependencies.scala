import sbt._

object Dependencies {
  private val SparkVersion = "2.4.4"
  private val SparkBQVersion = "0.13.1-beta"
  private val GCloudVersion = "1.80.0"
  private val HadoopGCSVersion = "1.6.1-hadoop2"
  private val QuillVersion = "3.5.0"
  private val ScalaTestVersion = "3.0.5"
  private val ScoptVersion = "3.7.1"
  private val ZioVersion = "1.0.0-RC18-2"
  private val DoobieVersion = "0.8.8"
  private val calibanVersion = "0.7.5"
  private val FlywayVersion = "6.4.1"

  lazy val loggingLibs = List(
    "com.jcabi" % "jcabi-log" % "0.17.4",
    "ch.qos.logback" % "logback-classic" % "1.2.3"
  )

  lazy val googleCloudLibs = List(
    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
    "com.google.cloud" % "google-cloud-bigquery" % GCloudVersion,
    "com.google.cloud" % "google-cloud-dataproc" % "0.122.1"
  )

  lazy val sparkLibs = List(
    "org.apache.spark" %% "spark-core" % SparkVersion,
    "org.apache.spark" %% "spark-sql" % SparkVersion,
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  )

  lazy val dbLibs = List(
    "io.getquill" %% "quill-jdbc"       % QuillVersion,
    "org.tpolecat" %% "doobie-core"     % DoobieVersion,
    "org.tpolecat" %% "doobie-postgres" % DoobieVersion,
    "org.tpolecat" %% "doobie-h2"       % DoobieVersion,
    "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
    "org.tpolecat" %% "doobie-quill"    % DoobieVersion,
    "org.flywaydb" % "flyway-core"      % FlywayVersion,
    "org.postgresql" % "postgresql"     % "42.2.8",
    "org.hsqldb"    % "hsqldb"          % "2.5.0"
  )

  lazy val zioLibs = List(
    "dev.zio" %% "zio" % ZioVersion,
    "dev.zio" %% "zio-interop-cats" % "2.0.0.0-RC11",
    "dev.zio" %% "zio-process" % "0.0.3"
  )

  lazy val miscLibs = List(
    "com.github.scopt" %% "scopt" % ScoptVersion
  )

  lazy val caliban = List(
    "com.github.ghostdogpr" %% "caliban" % calibanVersion,
    "com.github.ghostdogpr" %% "caliban-http4s" % calibanVersion
  )

  lazy val jwt = List(
    "com.pauldijou" %% "jwt-core" % "4.2.0"
  )

  lazy val testLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)
}
