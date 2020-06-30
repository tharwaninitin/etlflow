lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala212)

import Dependencies._

lazy val coreSettings = Seq(
  name := "etlflow-core",
  organization := "com.github.tharwaninitin",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= zioLibs ++ sparkLibs ++ googleCloudLibs ++ dbLibs ++ miscLibs ++ awsLibs ++ testLibs,
  excludeDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12",
    //"log4j" % "log4j"
  ),
  dependencyOverrides ++= {
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

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true)
  .aggregate(core, scheduler)

lazy val core = (project in file("modules/core"))
  .settings(coreSettings)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    initialCommands := "import etlflow._",
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      libraryDependencies in Compile,
      name, version, scalaVersion, sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etlflow",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
    ),
    Test / parallelExecution := false,
    testFrameworks += (new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val scheduler = (project in file("modules/scheduler"))
  .settings(schedulerSettings)
  .settings(
    scalacOptions ++= Seq("-Ypartial-unification"),
    Test / parallelExecution := false
  )
  .dependsOn(core)



