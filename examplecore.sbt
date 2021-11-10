import Versions._

lazy val examplecore = (project in file("examplecore"))
  .settings(
    name := "examplecore",
    crossScalaVersions := allScalaVersions,
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    )
  )