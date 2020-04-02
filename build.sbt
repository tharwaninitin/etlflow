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

lazy val etljobs = (project in file("etljobs"))
  .settings(etlJobsSettings)
  .enablePlugins(ClassDiagramPlugin)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
    initialCommands := "import etljobs._"
  )

lazy val examples = (project in file("examples"))
  .settings(examplesSettings)
  .settings(
    organization := "com.github.tharwaninitin",
    crossScalaVersions := supportedScalaVersions,
  )
  .dependsOn(etljobs)

