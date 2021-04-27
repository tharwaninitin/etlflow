import microsites.MicrositesPlugin.autoImport.micrositeName

scalaVersion := "2.12.10"

version in ThisBuild := "0.7.13"

enablePlugins(MicrositesPlugin)

lazy val etlflowCore = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#feature11"), "core")
lazy val etlflowServer = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#feature11"), "server")
lazy val etlflowSpark = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#feature11"), "spark")
lazy val etlflowCloud = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#feature11"), "cloud")

lazy val docsSettings = Seq(
  name := "etlflow-docs"
  , libraryDependencies ++= List("ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.apache.spark" %% "spark-sql" % "2.4.7" exclude("org.slf4j", "slf4j-log4j12"))
)

lazy val docs = (project in file("modules/docs"))
  .settings(docsSettings)
  .enablePlugins(MicrositesPlugin)
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true,
    organization := "com.github.tharwaninitin" ,
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
    mdocIn := (sourceDirectory in Compile).value / "mdoc"
  )
  .settings(
    mdocVariables := Map(
      "VERSION" -> "0.1.0",
      "DP_PROJECT_ID" -> "DP_PROJECT_ID",
      "DP_REGION" -> "DP_REGION",
      "DP_ENDPOINT" -> "DP_ENDPOINT",
      "DP_CLUSTER_NAME" -> "DP_CLUSTER_NAME",
      "DP_LIBS" -> "DP_LIBS",
      "DP_JOB_NAME" -> "DP_JOB_NAME",
      "DP_MAIN_CLASS" -> "DP_MAIN_CLASS"
    )
  )
  .dependsOn(
    etlflowCore,
    etlflowServer,
    etlflowCloud,
    etlflowSpark
  )
