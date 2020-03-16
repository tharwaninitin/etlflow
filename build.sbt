val SparkVersion = "2.4.4"
val GCloudVersion = "1.80.0"
val HadoopGCSVersion = "1.6.1-hadoop2"
val QuillVersion = "3.5.0"

version in ThisBuild := "0.7.5"

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

val sparkCore = "org.apache.spark" %% "spark-core" % SparkVersion
val sparkSql  = "org.apache.spark" %% "spark-sql" % SparkVersion
val quill     = "io.getquill" %% "quill-jdbc" % QuillVersion
val hadoopGCS = "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion
val gcloudBQ  = "com.google.cloud" % "google-cloud-bigquery" % GCloudVersion
val pg        = "org.postgresql" % "postgresql" % "42.2.8"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
val jcabi     = "com.jcabi" % "jcabi-log" % "0.17.4"

// To run test sequentially instead of default parallel execution
Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.CPU, 2),
  Tags.limit(Tags.Network, 10),
  Tags.limit(Tags.Test, 1),
  Tags.limitAll( 15 )
)

lazy val etljobsSettings = Seq(
  name := "etljobs"
  , libraryDependencies ++= Seq( 
    sparkCore % Provided, sparkSql % Provided,  // For Spark jobs in SparkSteps
    gcloudBQ % Provided,                        // For using Big-query java API in BQLoadStep
    hadoopGCS % Provided,                       // For saving and reading from GCS
    jcabi % Provided,
    quill % Provided,
    pg % Provided,
    scalaTest % Test
  )
)

lazy val examplesSettings = Seq(
  name := "examples"
  , libraryDependencies ++= Seq(
    sparkCore, sparkSql,  // For Spark jobs in SparkSteps
    gcloudBQ,             // For using Big-query java API in BQLoadStep
    hadoopGCS,            // For saving and reading from GCS
    scalaTest % Test
  )
)

val copyDocs = Def.taskKey[Unit]("copyDocs")

lazy val root = (project in file("."))
  .settings(
    copyDocs := {
      import java.nio.file.Files
      import java.nio.file.Paths
      import java.nio.file.StandardCopyOption.REPLACE_EXISTING
      //  import Path._
      //  val src = (Compile / crossTarget).value / "api"
      val src = new File("etljobs/etljobs/target/scala-2.12/api")
      println(src)
      val allFiles = src.listFiles()
      println(allFiles.length)
      def copyDir(from: java.nio.file.Path, to: java.nio.file.Path): Unit = {
        Files.copy(from, to, REPLACE_EXISTING)
      }

      // Files.delete(Paths.get("docs/api"))
      val from = Paths.get("etljobs/etljobs/target/scala-2.12/api")
      val to = Paths.get("docs/api")
      copyDir(from, to)
    },
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true)
  .aggregate(etljobs, examples)

lazy val etljobs = (project in file("etljobs"))
  .settings(etljobsSettings)
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

