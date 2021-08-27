import Versions._

lazy val examplecore = (project in file("examplecore"))
  .settings(
    name := "examplecore",
    organization := "com.github.tharwaninitin",
    crossScalaVersions := List(scala212, scala213,scala3),
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    )
  )