import Versions._
import ScalaCompileOptions._

lazy val examplejob = (project in file("examplejob"))
  .settings(
    name := "examplejob",
    crossScalaVersions := allScalaVersions,
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => s2copts
        case Some((3, _)) => s3copts
        case _ => Seq()
      }
    },
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-job" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    )
  )