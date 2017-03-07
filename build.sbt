name := "Mimir-Core"
version := "0.2-SNAPSHOT"
organization := "info.mimirdb"
scalaVersion := "2.10.5"

dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

scalacOptions ++= Seq(
  "-feature"
)

resolvers += "MimirDB" at "http://maven.mimirdb.info/"

libraryDependencies ++= Seq(
  "org.rogach"                 %%   "scallop"               % "0.9.5",
  "com.github.nscala-time"     %%   "nscala-time"           % "1.2.0",
  "ch.qos.logback"             %    "logback-classic"       % "1.1.7",
  "com.typesafe.scala-logging" %%   "scala-logging-slf4j"   % "2.1.2",
  "org.specs2"                 %%   "specs2-core"           % "3.8.4" % "test",
  "org.specs2"                 %%   "specs2-matcher-extra"  % "3.8.4" % "test",
  "org.specs2"                 %%   "specs2-junit"          % "3.8.4" % "test",
  ("nz.ac.waikato.cms.weka"    %    "weka-stable"           % "3.8.1").
    exclude("nz.ac.waikato.cms.weka", "weka-dev").
    exclude("nz.ac.waikato.cms.weka.thirdparty", "java-cup-11b-runtime"),
  ("nz.ac.waikato.cms.moa"     %    "moa"                   % "2014.11").
    exclude("nz.ac.waikato.cms.weka", "weka-dev").
    exclude("nz.ac.waikato.cms.weka.thirdparty", "java-cup-11b-runtime"),
  "org.apache.lucene"          %    "lucene-spellchecker"   % "3.6.2",
  "org.xerial"                 %    "sqlite-jdbc"           % "3.14.2.1",
  "info.mimirdb"               %    "jsqlparser"            % "1.0.0",
  "org.apache.commons"         %    "commons-csv"           % "1.4", 
  "commons-io"                 %    "commons-io"            % "2.5"
)

lazy val parser = taskKey[Unit]("Builds the SQL Parser")

parser := {
  val logger = streams.value.log
  Process(List(
    "rm", "-f",
    "src/main/java/mimir/parser/MimirJSqlParser.java",
    "src/main/java/mimir/parser/MimirJSqlParserConstants.java",
    "src/main/java/mimir/parser/MimirJSqlParserTokenManager.java",
    "src/main/java/mimir/parser/ParseException.java",
    "src/main/java/mimir/parser/SimpleCharStream.java",
    "src/main/java/mimir/parser/Token.java",
    "src/main/java/mimir/parser/TokenMgrError.java"
  )) ! logger match {
    case 0 => // Success
    case n => sys.error(s"Could not clean up after old SQL Parser: $n")
  }
  Process(List(
    "java -cp lib/javacc.jar javacc",
    "-OUTPUT_DIRECTORY=src/main/java/mimir/parser",
    "src/main/java/mimir/parser/JSqlParserCC.jj"
  ).mkString(" ")) ! logger match {
    case 0 => // Success
    case n => sys.error(s"Could not build SQL Parser: $n")
  }
}

scalacOptions in Test ++= Seq("-Yrangepos")

parallelExecution in Test := false

resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

fork := true

testOptions in Test ++= Seq( Tests.Argument("junitxml"), Tests.Argument("console") )

////// Assembly Plugin //////
// We use the assembly plugin to create self-contained jar files
// https://github.com/sbt/sbt-assembly

test in assembly := {}
assemblyJarName in assembly := "Mimir.jar"
mainClass in assembly := Some("mimir.Mimir")

////// Publishing Metadata //////
// use `sbt publish make-pom` to generate 
// a publishable jar artifact and its POM metadata

publishMavenStyle := true

pomExtra := (
  <url>http://mimirdb.info</url>
  <licenses>
    <license>
      <name>Apache License 2.0</name>
      <url>http://www.apache.org/licenses/</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:ubodin/mimir.git</url>
    <connection>scm:git:git@github.com:ubodin/mimir.git</connection>
  </scm>)

/////// Publishing Options ////////
// use `sbt publish` to update the package in 
// your own local ivy cache
publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))