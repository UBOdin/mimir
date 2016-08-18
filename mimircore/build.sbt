name := "Mimir"

version := "0.1"

scalaVersion := "2.10.5"

dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

scalacOptions ++= Seq(
  "-feature"
)

libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.2.0"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.8.4" % "test"
libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % "3.8.4" % "test"
libraryDependencies += "org.specs2" %% "specs2-junit" % "3.8.4" % "test"

libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

// libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

lazy val parser = taskKey[Unit]("Builds the SQL Parser")

parser := {
  val logger = streams.value.log
  Process("rm -f src/main/java/mimir/parser/*.java") ! logger match {
    case 0 => // Success
    case n => sys.error(s"Could not clean up after old SQL Parser: $n")
  }
  Process(List(
    "java -cp mimircore/lib/javacc.jar javacc",
    "-OUTPUT_DIRECTORY=mimircore/src/main/java/mimir/parser",
    "mimircore/src/main/java/mimir/parser/JSqlParserCC.jj"
  ).mkString(" ")) ! logger match {
    case 0 => // Success
    case n => sys.error(s"Could not build SQL Parser: $n")
  }
}

scalacOptions in Test ++= Seq("-Yrangepos")

parallelExecution in Test := false

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies

resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

fork := true

testOptions in Test ++= Seq( Tests.Argument("junitxml"), Tests.Argument("console") )