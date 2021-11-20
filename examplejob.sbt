import Versions._

lazy val examplejob = (project in file("examplejob"))
  .settings(
    name := "examplejob",
    crossScalaVersions := allScalaVersions,
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-job" % EtlFlowVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    )
  )