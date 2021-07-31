import NativePackagerHelper._
import Dependencies._

lazy val scala212 = "2.12.13"
lazy val scala213 = "2.13.6"
lazy val scala2Versions = List(scala212 ,scala213)

val EtlFlowVersion = "0.10.0"

lazy val loggerTask = TaskKey[Unit]("loggerTask")

lazy val examples = (project in file("examples"))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "examples",
    organization := "com.github.tharwaninitin",
    crossScalaVersions := List(scala212, scala213),
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-server" % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-cloud" % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-http" % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-redis" % EtlFlowVersion,
      "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
      "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
      "org.apache.hadoop" % "hadoop-common" % HadoopS3Version,
      "ch.qos.logback" % "logback-classic" % LogbackVersion,
      "org.postgresql" % "postgresql" % PgVersion
    ),
    loggerTask := {
      val logger = org.slf4j.LoggerFactory.getLogger("sbt")
      println(logger.getClass.getName)
    },
    dependencyOverrides ++= {
      Seq(
        "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.7.1",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
      )
    },
    excludeDependencies ++= Seq(
      "org.slf4j" % "slf4j-log4j12",
      //"log4j" % "log4j"
    ),
    Docker / packageName  := "etlflow",
    Compile / mainClass := Some("examples.RunServer"),
    dockerBaseImage := "openjdk:jre",
    dockerExposedPorts ++= Seq(8080),
    maintainer := "tharwaninitin182@gmail.com",
    Universal / mappings ++= directory(sourceDirectory.value / "main" / "data"),
    Test / parallelExecution := false,
  )