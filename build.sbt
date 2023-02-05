import Dependencies._
import ScalaCompileOptions._
import Versions._

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val commonSettings = Seq(
  scalaVersion               := Scala212,
  dependencyUpdatesFailBuild := true,
  dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang"),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => s2copts ++ s212copts
      case Some((2, 13)) => s2copts
      case Some((3, _))  => s3copts
      case _             => Seq()
    }
  },
  Compile / compile / wartremoverErrors ++= Warts.allBut(
    Wart.Any,
    Wart.DefaultArguments,
    Wart.Nothing,
    Wart.Equals,
    Wart.FinalCaseClass,
    Wart.Overloading,
    Wart.StringPlusAny
  ),
  Test / parallelExecution := false,
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
)

lazy val coreSettings = Seq(
  name               := "etlflow-core",
  crossScalaVersions := AllScalaVersions,
  libraryDependencies ++= coreLibs ++ coreTestLibs
)

lazy val sparkSettings = Seq(
  name               := "etlflow-spark",
  crossScalaVersions := Scala2Versions,
  libraryDependencies ++= sparkLibs ++ coreTestLibs ++ jdbcTestLibs ++ sparkTestLibs
)

lazy val jdbcSettings = Seq(
  name               := "etlflow-jdbc",
  crossScalaVersions := AllScalaVersions,
  libraryDependencies ++= jdbcLibs ++ coreTestLibs ++ jdbcTestLibs
)

lazy val httpSettings = Seq(
  name                       := "etlflow-http",
  crossScalaVersions         := AllScalaVersions,
  dependencyUpdatesFailBuild := false,
  libraryDependencies ++= httpLibs ++ coreTestLibs
)

lazy val redisSettings = Seq(
  name               := "etlflow-redis",
  crossScalaVersions := Scala2Versions,
  libraryDependencies ++= redisLibs ++ coreTestLibs
)

lazy val emailSettings = Seq(
  name               := "etlflow-email",
  crossScalaVersions := AllScalaVersions,
  libraryDependencies ++= emailLibs ++ coreTestLibs
)

lazy val awsSettings = Seq(
  name               := "etlflow-aws",
  crossScalaVersions := AllScalaVersions,
  libraryDependencies ++= awsLibs ++ coreTestLibs
)

lazy val gcpSettings = Seq(
  name               := "etlflow-gcp",
  crossScalaVersions := AllScalaVersions,
  libraryDependencies ++= gcpLibs ++ coreTestLibs
)

lazy val k8sSettings = Seq(
  name               := "etlflow-k8s",
  crossScalaVersions := AllScalaVersions,
  libraryDependencies ++= k8sLibs ++ coreTestLibs
)

lazy val ftpSettings = Seq(
  name               := "etlflow-ftp",
  crossScalaVersions := Scala2Versions,
  libraryDependencies ++= ftpLibs ++ coreTestLibs
)

lazy val etlflow = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip     := true
  )
  .aggregate(core.js, core.jvm, spark, jdbc, http, redis, email, aws, gcp, k8s, ftp)

lazy val core =
  (crossProject(JSPlatform, JVMPlatform).withoutSuffixFor(JVMPlatform).crossType(CrossType.Pure) in file("modules/core"))
    .settings(commonSettings)
    .settings(coreSettings)

lazy val jdbc = (project in file("modules/jdbc"))
  .settings(commonSettings)
  .settings(jdbcSettings)
  .dependsOn(core.jvm)

lazy val spark = (project in file("modules/spark"))
  .settings(commonSettings)
  .settings(sparkSettings)
  .dependsOn(core.jvm)

lazy val http = (project in file("modules/http"))
  .settings(commonSettings)
  .settings(httpSettings)
  .dependsOn(core.jvm)

lazy val redis = (project in file("modules/redis"))
  .settings(commonSettings)
  .settings(redisSettings)
  .dependsOn(core.jvm)

lazy val email = (project in file("modules/email"))
  .settings(commonSettings)
  .settings(emailSettings)
  .dependsOn(core.jvm)

lazy val aws = (project in file("modules/aws"))
  .settings(commonSettings)
  .settings(awsSettings)
  .dependsOn(core.jvm)

lazy val gcp = (project in file("modules/gcp"))
  .settings(commonSettings)
  .settings(gcpSettings)
  .dependsOn(core.jvm)

lazy val k8s = (project in file("modules/k8s"))
  .settings(commonSettings)
  .settings(k8sSettings)
  .dependsOn(core.jvm)

lazy val ftp = (project in file("modules/ftp"))
  .settings(commonSettings)
  .settings(ftpSettings)
  .dependsOn(core.jvm)

lazy val docs = project
  .in(file("modules/docs")) // important: it must not be docs/
  .dependsOn(core.jvm, spark, jdbc, http, redis, email, aws, gcp, k8s)
  .settings(
    name           := "etlflow-docs",
    publish / skip := true,
    mdocVariables  := Map("VERSION" -> version.value, "Scala212" -> Scala212, "Scala213" -> Scala213, "Scala3" -> Scala3),
    mdocIn         := new File("docs/readme.template.md"),
    mdocOut        := new File("README.md")
  )
  .enablePlugins(MdocPlugin)
