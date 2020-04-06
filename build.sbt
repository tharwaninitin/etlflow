version in ThisBuild := "0.7.6"

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

// To run test sequentially instead of default parallel execution
Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.CPU, 2),
  Tags.limit(Tags.Network, 10),
  Tags.limit(Tags.Test, 1),
  Tags.limitAll( 15 )
)

import Dependencies._
lazy val etlJobsSettings = Seq(
  name := "etljobs"
  , libraryDependencies ++= sparkLibs ++ googleCloudLibs ++ loggingLibs ++ dbLibs ++ miscLibs ++ testLibs
)

lazy val examplesSettings = Seq(
  name := "examples"
  , libraryDependencies ++= sparkLibs ++ googleCloudLibs ++ loggingLibs ++ dbLibs ++ miscLibs ++ testLibs
)

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true)
  .aggregate(etljobs, examples)

import NativePackagerHelper._

lazy val etljobs = (project in file("etljobs"))
  .settings(etlJobsSettings)
  .enablePlugins(ClassDiagramPlugin)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    packageName in Docker := "etljobs",
    mainClass in Compile := Some("etljobs.examples.LoadData"),
    dockerBaseImage := "openjdk:jre",
    maintainer := "tharwaninitin182@gmail.com",
    // https://stackoverflow.com/questions/40511337/how-copy-resources-files-with-sbt-docker-plugin
    mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
    mappings in Universal ++= directory(sourceDirectory.value / "main" / "data"),
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
  )

lazy val examples = (project in file("examples"))
  .settings(examplesSettings)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
  )
  .dependsOn(etljobs)

