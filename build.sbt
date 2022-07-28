import sbtassembly.AssemblyPlugin.autoImport.{assemblyMergeStrategy, assemblyOption}

scalaVersion := "2.13.8"

/* assembly / mainClass := Some("com.AppDemo") */

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"     % "3.3.0",
  "org.apache.spark" %% "spark-sql"      % "3.3.0",
  "org.apache.spark" %% "spark-mllib"    % "3.3.0",
  "org.scalactic"    %% "scalactic"      % "3.1.4",
  "org.scalatest"    %% "scalatest"      % "3.2.12" % Test,
  "com.amazonaws"     % "aws-java-sdk"   % "1.11.375",
  "org.apache.hadoop" % "hadoop-common"  % "3.2.4",
  "org.apache.hadoop" % "hadoop-aws"     % "3.2.4",
  "com.databricks"    % "spark-xml_2.12" % "0.14.0"
  /* "org.apache.hadoop" % "hadoop-client" % "3.2.0", */
  /* "com.google.guava"  % "guava"         % "31.1-jre" */
  /* "com.fasterxml.jackson.core"    % "jackson-databind"     % "2.12.2", */
  /* "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2", */
  /* "org.apache.spark" %% "spark-hive"    % "3.1.1" % "provided", */
)

lazy val root = project
  .in(file("."))
  .settings(
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
        .withIncludeDependency(false)
    },
    Seq(
      assembly / test     := {},
      assembly / logLevel := Level.Info,
      assembly / assemblyMergeStrategy := {
        case PathList("META-INF", xs @ _*) => xs match {
            case ps @ (x :: xs)
                if ps.last.endsWith(".SF") || ps.last.endsWith(".DSA") ||
                  ps.last.endsWith(".RSA") => MergeStrategy.discard
            case ps @ (x :: xs) if ps.last.endsWith("org.apache.hadoop.fs.FileSystem") =>
              MergeStrategy.filterDistinctLines
            case ps @ (x :: xs)
                if ps.last.endsWith("org.apache.spark.sql.sources.DataSourceRegister") =>
              MergeStrategy.filterDistinctLines
            case ps @ (x :: xs)
                if ps.last.endsWith("INDEX.LIST") || ps.last.endsWith("MANIFEST.MF") ||
                  ps.last.endsWith("LICENSE") || ps.last.endsWith("LICENSE.txt") ||
                  ps.last.endsWith("DEPENDENCIES") || ps.last.endsWith("NOTICE") ||
                  ps.last.endsWith("NOTICE.txt") || ps.last.endsWith("pom.properties") ||
                  ps.last.endsWith("pom.xml") => MergeStrategy.rename
            case _ => MergeStrategy.first
          }
        case PathList("reference.conf")   => MergeStrategy.discard
        case PathList("application.conf") => MergeStrategy.discard
        case x                            => MergeStrategy.last
      },
      assembly / assemblyOutputPath := file(s"target/${(assembly / assemblyJarName).value}"),
      assembly / assemblyJarName    := name.value + ".jar",
      Universal / mappings := {
        val universalMappings = (Universal / mappings).value
        val fatJar            = (Compile / assembly).value
        val filtered = universalMappings.filter { case (file, name) => !name.endsWith(".jar") }
        filtered :+ fatJar -> ("lib/" + fatJar.getName)
      },
      scriptClasspath := Seq((assembly / assemblyJarName).value)
    )
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(AssemblyPlugin)
