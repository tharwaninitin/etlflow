import Versions._
import NativePackagerHelper._

lazy val examplecore = (project in file("examplecore"))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "examplecore",
    crossScalaVersions := allScalaVersions,
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    ),
    Docker / packageName  := "etlflow-core",
    dockerBaseImage := "openjdk:jre",
    dockerExposedPorts ++= Seq(8080),
    maintainer := "tharwaninitin182@gmail.com",
    Universal / mappings ++= directory(sourceDirectory.value / "main" / "data"),
  )