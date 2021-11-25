import ScalaCompileOptions._
import Versions._

lazy val examplecore = (project in file("examplecore"))
  .settings(
    name := "examplecore",
    crossScalaVersions := allScalaVersions,
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => s2copts
        case Some((3, _)) => s3copts
        case _ => Seq()
      }
    },
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-db" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    )
  )