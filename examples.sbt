import NativePackagerHelper._

val SparkBQVersion = "0.13.1-beta"
val HadoopGCSVersion = "1.6.1-hadoop2"
val HadoopS3Version = "2.10.0"
val LogbackVersion = "1.2.3"
val EtlFlowVersion = "0.7.14"
val PgVersion = "42.2.8"

lazy val loggerTask = TaskKey[Unit]("loggerTask")

lazy val examples = (project in file("examples"))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "etlflow-examples",
    organization := "com.github.tharwaninitin",
    scalaVersion := "2.12.10",
    libraryDependencies ++= List(
        "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
        "com.github.tharwaninitin" %% "etlflow-scheduler" % EtlFlowVersion,
        "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion,
        "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
        "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
        "org.apache.hadoop" % "hadoop-common" % HadoopS3Version,
        "ch.qos.logback" % "logback-classic" % LogbackVersion,
        "org.postgresql" % "postgresql" % PgVersion,
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
    packageName in Docker := "etlflow",
    mainClass in Compile := Some("examples.RunCustomServer"),
    dockerBaseImage := "openjdk:jre",
    dockerExposedPorts ++= Seq(8080),
    maintainer := "tharwaninitin182@gmail.com",
    // https://stackoverflow.com/questions/40511337/how-copy-resources-files-with-sbt-docker-plugin
    mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
    mappings in Universal ++= directory(sourceDirectory.value / "main" / "data"),
    Test / parallelExecution := false
  )