lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12" // not supported now
lazy val supportedScalaVersions = List(scala212)

import Dependencies._

lazy val coreSettings = Seq(
  name := "etlflow-core"
  , libraryDependencies ++= zioLibs ++ sparkLibs ++ googleCloudLibs
    ++ loggingLibs ++ dbLibs ++ miscLibs
    ++ awsLibs ++ testLibs
  , dependencyOverrides ++= {
    Seq(
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
    )
  }
)

lazy val schedulerSettings = Seq(
  name := "etlflow-scheduler"
  , libraryDependencies ++= caliban ++ jwt
)

lazy val examplesSettings = Seq(
  name := "etlflow-examples"
)

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true)
  .aggregate(core, scheduler, examples)

lazy val core = (project in file("modules/core"))
  .settings(coreSettings)
  .enablePlugins(ClassDiagramPlugin)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(MicrositesPlugin)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
    initialCommands := "import etlflow._",
    micrositeName := "EtlFlow",
    micrositeDescription := "Functional library in Scala for writing ETL jobs",
    micrositeUrl := "https://tharwaninitin.github.io/etlflow/site/",
    micrositeTheme := "pattern",
    micrositeBaseUrl := "/etlflow/site",
    micrositeDocumentationUrl := "/etlflow/site/docs/",
    micrositeGithubOwner := "tharwaninitin",
    micrositeGithubRepo := "etlflow",
    micrositeGitterChannel := false,
    micrositeDocumentationLabelDescription := "Documentation",
    micrositeCompilingDocsTool := WithMdoc,
    // micrositeDataDirectory := (resourceDirectory in Compile).value / "docs" / "data",
    micrositeAuthor := "Nitin Tharwani",
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      libraryDependencies in Compile,
      name, version, scalaVersion, sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etlflow",
    Test / parallelExecution := false,
    testFrameworks += (new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val scheduler = (project in file("modules/scheduler"))
  .settings(schedulerSettings)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
    scalacOptions ++= Seq("-Ypartial-unification"),
    Test / parallelExecution := false
  )
  .dependsOn(core)

import NativePackagerHelper._

lazy val examples = (project in file("modules/examples"))
  .settings(examplesSettings)
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
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
  .dependsOn(core, scheduler)

