
import mill._
import scalalib._
import scalafmt._
import $ivy.`com.lihaoyi::mill-contrib-buildinfo:$MILL_VERSION`
import $ivy.`com.lihaoyi::mill-contrib-docker:$MILL_VERSION`
import mill.contrib.buildinfo.BuildInfo
import contrib.docker.DockerModule
import publish._
import $file.project.Dependencies
import Dependencies.Dependencies._


trait CommonModule extends ScalaModule with PublishModule  {
  def scalaVersion = "2.12.10"
  def publishVersion = "0.7.19"
  def pomSettings = PomSettings(
    description = "Functional, Composable library in Scala for writing ETL jobs",
    organization = "com.github.tharwaninitin",
    url = "https://github.com/tharwaninitin/etlflow",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("tharwaninitin", "https://github.com/tharwaninitin/etlflow"),
    developers = Seq(
      Developer("tharwaninitin", "Nitin Tharwani","https://github.com/tharwaninitin")
    )
  )
}

object modules extends ScalaModule with ScalafmtModule  with CommonModule {

  object core extends ScalaModule with CommonModule with BuildInfo {
    def name = "etlflow-core"
    override  def artifactName = "etlflow-core"
    override def buildInfoPackageName = Some("etlflow")
    override  def  buildInfoMembers: T[Map[String, String]] = T {
      Map(
        "name" -> name,
        "version" -> publishVersion(),
        "scalaVersion" -> scalaVersion()
      )
    }
    override def ivyDeps = zioLibs ++ dbLibs ++ catsLibs ++ jsonLibs++ miscLibs ++ redis ++ scalajHttp ++ mail ++ coreTestLibs
    def testFrameworks = Seq("zio.test.sbt.ZTestFramework")
  }

  object spark extends ScalaModule with CommonModule {
    override  def artifactName = "etlflow-spark"
    override def moduleDeps = Seq(core)
    override def ivyDeps = sparkLibs ++ cloudTestLibs
    def testFrameworks = Seq("zio.test.sbt.ZTestFramework")
  }

  object cloud extends ScalaModule with CommonModule {
    override  def artifactName = "etlflow-cloud"
    override def moduleDeps = Seq(core)
    override def ivyDeps = streamingLibs ++ googleCloudLibs ++ awsLibs ++ cloudTestLibs
    def testFrameworks = Seq("zio.test.sbt.ZTestFramework")
  }

  object scheduler extends ScalaModule with CommonModule with BuildInfo {
    override  def artifactName = "etlflow-scheduler"
    override def moduleDeps = Seq(cloud)
    override def buildInfoPackageName = Some("etlflow")
    val now = java.time.Instant.now().toEpochMilli
    // Output the build time with the local timezone suffix
    val dtf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val nowStr = dtf.format(now)
    override  def  buildInfoMembers: T[Map[String, String]] = T {
      Map(
        "name" -> "etlflow-scheduler",
        "version" -> publishVersion(),
        "scalaVersion" -> scalaVersion(),
        "builtAtString" -> nowStr
      )
    }

    override def ivyDeps = caliban ++ jwt ++ kubernetes ++ http4sclient ++ coreTestLibs
    def testFrameworks = Seq("zio.test.sbt.ZTestFramework")
    val defaultScalaOpts = Seq(
      "-deprecation", // Emit warning and location for usages of deprecated APIs.
      "-encoding", "UTF-8", // Specify character encoding used by source files.
      "-language:higherKinds", // Allow higher-kinded types
      "-language:postfixOps", // Allows operator syntax in postfix position (deprecated since Scala 2.10)
      "-feature", // Emit warning and location for usages of features that should be imported explicitly.
      "-Ypartial-unification"      // Enable partial unification in type constructor inference
    )
    override  def scalacOptions = defaultScalaOpts
  }
}

object examples extends SbtModule with ScalafmtModule with CommonModule with DockerModule {

  override def moduleDeps = Seq(modules.core, modules.spark, modules.scheduler)

  override def ivyDeps = {
    val EtlFlowVersion = "0.7.19"
    val SparkBQVersion = "0.16.1"
    val HadoopGCSVersion = "1.6.1-hadoop2"
    val HadoopS3Version = "2.10.0"
    val PgVersion = "42.2.8"
    val LogbackVersion = "1.2.3"

    Agg(
      ivy"com.github.tharwaninitin::etlflow-core:$EtlFlowVersion".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"com.github.tharwaninitin::etlflow-scheduler:$EtlFlowVersion".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"com.github.tharwaninitin::etlflow-spark:$EtlFlowVersion".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"com.github.tharwaninitin::etlflow-cloud:$EtlFlowVersion".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"com.google.cloud.spark::spark-bigquery-with-dependencies:$SparkBQVersion".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"com.google.cloud.bigdataoss:gcs-connector:$HadoopGCSVersion".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"org.apache.hadoop:hadoop-aws:$HadoopS3Version".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"org.apache.hadoop:hadoop-common:$HadoopS3Version".exclude("org.slf4j" -> "slf4j-log4j12"),
      ivy"ch.qos.logback:logback-classic:$LogbackVersion",
      ivy"org.postgresql:postgresql:$PgVersion".exclude("org.slf4j" -> "slf4j-log4j12")
    )
  }

  object docker extends DockerConfig {
    override def tags = List("")
    override def baseImage = "openjdk:14.0.2"
    override def pullBaseImage = true
  }

  //with mill you need to specify the main class when you have more than one
  override  def mainClass = Some("examples.RunServer")
}