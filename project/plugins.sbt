// Code Quality Plugins
addSbtPlugin("org.wartremover" % "sbt-wartremover"       % "3.0.6")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scalameta"   % "sbt-scalafmt"          % "2.4.6")

// Type Checked Documentation Plugin
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.3.6")

// Scala JS Integration Plugins
addSbtPlugin("org.scala-js"       % "sbt-scalajs"              % "1.12.0")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.2.0")

// Publishing Plugins
addSbtPlugin("io.crashbox"    % "sbt-gpg"            % "0.2.1")
addSbtPlugin("com.github.sbt" % "sbt-release"        % "1.1.0")
addSbtPlugin("ch.epfl.scala"  % "sbt-version-policy" % "2.0.1")

// Other Plugins
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"  % "1.2.0")
addSbtPlugin("org.scoverage"    % "sbt-scoverage" % "1.9.3")
addSbtPlugin("com.timushev.sbt" % "sbt-updates"   % "0.6.3")
