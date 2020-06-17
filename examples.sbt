import NativePackagerHelper._

lazy val examples = (project in file("modules/examples"))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "etlflow-examples",
    organization := "com.github.tharwaninitin",
    scalaVersion := "2.12.10",
    libraryDependencies ++= List(
        "com.github.tharwaninitin" %% "etlflow-core" % "0.7.14",
        "com.github.tharwaninitin" %% "etlflow-scheduler" % "0.7.14",
    ),
    packageName in Docker := "etlflow",
    // mainClass in Compile := Some("examples.LoadData"),
    mainClass in Compile := Some("examples.RunServer"),
    dockerBaseImage := "openjdk:jre",
    dockerExposedPorts ++= Seq(8080),
    maintainer := "tharwaninitin182@gmail.com",
    // https://stackoverflow.com/questions/40511337/how-copy-resources-files-with-sbt-docker-plugin
    mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
    mappings in Universal ++= directory(sourceDirectory.value / "main" / "data"),
    Test / parallelExecution := false
  )