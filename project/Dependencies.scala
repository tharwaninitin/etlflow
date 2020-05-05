import sbt._

object Dependencies {
  private val SparkVersion = "2.4.4"
  private val SparkBQVersion = "0.13.1-beta"
  private val GCloudVersion = "1.80.0"
  private val HadoopGCSVersion = "1.6.1-hadoop2"
  private val QuillVersion = "3.5.0"
  private val ScalaTestVersion = "3.0.5"
  private val ScoptVersion = "3.7.1"

  lazy val loggingLibs = List(
    "com.jcabi" % "jcabi-log" % "0.17.4"
  )

  lazy val googleCloudLibs = List(
    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
    "com.google.cloud" % "google-cloud-bigquery" % GCloudVersion
  )

  lazy val sparkLibs = List(
    "org.apache.spark" %% "spark-core" % SparkVersion,
    "org.apache.spark" %% "spark-sql" % SparkVersion,
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  )

  lazy val dbLibs = List(
    "io.getquill" %% "quill-jdbc" % QuillVersion,
    "org.postgresql" % "postgresql" % "42.2.8"
  )

  lazy val miscLibs = List(
    "com.github.scopt" %% "scopt" % ScoptVersion
  )

  lazy val testLibs = List(
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)
}