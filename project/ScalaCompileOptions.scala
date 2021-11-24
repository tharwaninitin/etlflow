object ScalaCompileOptions {
  val s2copts: Seq[String] = Seq(
    "-unchecked"
    , "-feature"
    , "-deprecation"
    , "-language:higherKinds"
    , "-Ywarn-unused:implicits"             // Warn if an implicit parameter is unused.
    , "-Ywarn-unused:imports"               // Warn if an import selector is not referenced.
    , "-Ywarn-unused:locals"                // Warn if a local definition is unused.
    , "-Xfatal-warnings"
  )
  val s3copts: Seq[String] = Seq(
    "-unchecked"
    , "-feature"
    , "-deprecation"
  )
}
