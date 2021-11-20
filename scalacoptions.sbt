ThisBuild / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) => Seq(
      "-unchecked"
      , "-feature"
      , "-deprecation"
      , "-language:higherKinds"
      , "-Ywarn-unused:implicits"             // Warn if an implicit parameter is unused.
      , "-Ywarn-unused:imports"               // Warn if an import selector is not referenced.
      , "-Ywarn-unused:locals"                // Warn if a local definition is unused.
      , "-Xfatal-warnings"
    )
    case Some((3, _)) => Seq(
      "-unchecked"
      , "-feature"
      , "-deprecation"
    )
    case _ => Seq()
  }
}