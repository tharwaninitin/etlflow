scalaVersion := "2.12.10"

version in ThisBuild := "0.7.13"

enablePlugins(MicrositesPlugin)

organization := "com.github.tharwaninitin"
micrositeName := "EtlFlow"
micrositeDescription := "Functional library in Scala for writing ETL jobs"
micrositeUrl := "https://tharwaninitin.github.io/etlflow/site/"
micrositeTheme := "pattern"
micrositeBaseUrl := "/etlflow/site"
micrositeDocumentationUrl := "/etlflow/site/docs/"
micrositeGithubOwner := "tharwaninitin"
micrositeGithubRepo := "etlflow"
micrositeGitterChannel := false
micrositeDocumentationLabelDescription := "Documentation"
micrositeCompilingDocsTool := WithMdoc

