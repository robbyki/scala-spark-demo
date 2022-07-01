addSbtPlugin("ch.epfl.scala"  % "sbt-bloop"           % "1.4.13")
addSbtPlugin("com.eed3si9n"   % "sbt-assembly"        % "1.2.0")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.0")
addSbtPlugin(("org.scalameta" % "sbt-scalafmt" % "2.4.2").exclude("io.get-coursier", "interface"))
