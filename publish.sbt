ThisBuild / organization := "com.github.tharwaninitin"
ThisBuild / organizationName := "github"
ThisBuild / organizationHomepage := Some(url("https://github.com/tharwaninitin/etljobs"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/tharwaninitin/etljobs"),
    "scm:git@github.com:tharwaninitin/etljobs.git"
  )
)
ThisBuild / developers := List(Developer("tharwaninitin",
                             "Nitin Tharwani",
                             "tharwaninitin182@gmail.com",
                             url("https://github.com/tharwaninitin")))
ThisBuild / description := "Functional, Composable library in Scala for writing ETL jobs"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/tharwaninitin/etljobs"))

// Add sonatype repository settings
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true