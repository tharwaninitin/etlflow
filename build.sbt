version in ThisBuild := "0.7.7"

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12" // not supported now
lazy val supportedScalaVersions = List(scala212)

import Dependencies._

lazy val coreSettings = Seq(
  name := "etljobs-core"
  , libraryDependencies ++= zioLibs ++ sparkLibs ++ googleCloudLibs ++ loggingLibs ++ dbLibs ++ miscLibs ++ testLibs
)

lazy val examplesSettings = Seq(
  name := "etljobs-examples"
  , libraryDependencies ++= zioLibs ++ sparkLibs ++ googleCloudLibs ++ loggingLibs ++ dbLibs ++ miscLibs ++ testLibs
)

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true)
  .aggregate(core, examples)

lazy val core = (project in file("modules/core"))
  .settings(coreSettings)
  .enablePlugins(ClassDiagramPlugin)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
    initialCommands := "import etljobs._",
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      libraryDependencies in Compile,
      name, version, scalaVersion, sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etljobs",
    Test / parallelExecution := false
  )

import NativePackagerHelper._

lazy val examples = (project in file("modules/examples"))
  .settings(examplesSettings)
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
    packageName in Docker := "etljobs",
    mainClass in Compile := Some("examples.LoadData"),
    dockerBaseImage := "openjdk:jre",
    maintainer := "tharwaninitin182@gmail.com",
    // https://stackoverflow.com/questions/40511337/how-copy-resources-files-with-sbt-docker-plugin
    mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
    mappings in Universal ++= directory(sourceDirectory.value / "main" / "data"),
    Test / parallelExecution := false
  )
  .dependsOn(core)

