import NativePackagerHelper._
import ScalaCompileOptions._
import Versions._

lazy val exampleserver = (project in file("exampleserver"))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "exampleserver",
    crossScalaVersions := allScalaVersions,
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => s2copts
        case Some((3, _)) => s3copts
        case _ => Seq()
      }
    },
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-server" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    ),
    Docker / packageName  := "etlflow-server",
    dockerBaseImage := "openjdk:jre",
    dockerExposedPorts ++= Seq(8080),
    maintainer := "tharwaninitin182@gmail.com",
    Universal / mappings ++= directory(sourceDirectory.value / "main" / "data"),
  )