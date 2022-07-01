addSbtPlugin("ch.epfl.scala"    % "sbt-bloop"           % "1.5.2")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"        % "1.2.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")
addSbtPlugin(("org.scalameta" % "sbt-scalafmt" % "2.4.6").exclude("io.get-coursier", "interface"))
