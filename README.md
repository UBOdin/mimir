# The Mimir Data-Ish Exploration Tool

One of the biggest costs in analytics is data wrangling: getting your messy, mis-labeled, disorganized data into a format that you can actually ask questions about. Unfortunately, most tools for data wrangling force you to do all of this work upfront — before you actually know what you even want to do with the data.

Mimir is about getting you to your analysis as fast as possible.  It lets you harness the raw power of SQL, but also provides a ton of powerful langauge extensions:
* Stop messing with data import and relational schema design.  The versatile [`LOAD`](https://github.com/UBOdin/mimir/wiki/Mimir-SQL#load) command allows you to quickly transform documents into relational tables without the muss and fuss of upfront schema design or defining complex transformation operators.
* Stop writing messy scripts to visualize your data.  The  (soon™ to be released) [`PLOT`](https://github.com/UBOdin/mimir/wiki/Mimir-SQL#plot) command lets you take SQL queries and see them directly – notebook style, PDF/PNG, or Javascript, take your pick.
* Stop writing complex ETL pipelines for simple data.  [Lenses](https://github.com/UBOdin/mimir/wiki/Lenses-and-Adaptive-Schemas) do the same work, but don't require nearly as much configuration (and we're doing more every day to make lenses easier to use).

Unlike most other SQL-based systems, Mimir lets you make decisions during and after data exploration.  All of Mimir's functionality is based on three ideas: (1) Mimir provides sensible *best guess* defaults, and (2) Mimir warns you when one of its guesses is going to affect what it's telling you, and (3) Mimir lets you easily inspect what it's doing to your data with the [`ANALYZE`](https://github.com/UBOdin/mimir/wiki/Mimir-SQL#analyze) query command.

Better still, you don't need any new infrastructure.  Mimir attaches to ordinary relational databases through JDBC (We currently support SQLite, with SparkSQL and Oracle support in progress).  If you don't care, Mimir just puts everything in a super portable SQLite database by default.

## Quick-Start

#### Install with Homebrew

```
$> brew tap UBOdin/ubodin
$> brew install mimir
$> mimir --help
```

#### Manually download the JAR 
Download the latest version of Mimir:
* [0.2 (Nightly Build)](http://maven.mimirdb.info/mimirdb/mimir-core_2.10/0.2-SNAPSHOT/Mimir.jar)
* [0.1 (Stable)](http://maven.mimirdb.info/mimirdb/mimir-core_2.10/0.1/Mimir.jar)

This is a self-contained jar.  Run it with
```
$> java -jar Mimir.jar
```

#### Link with SBT (or Maven)

Add the following to your build.sbt
```
resolvers += "MimirDB" at "http://maven.mimirdb.info/"
libraryDependencies += "info.mimirdb" %% "mimir" % "0.2-SNAPSHOT"
```


## User Guides

Mimir adds some useful language features to SQL.  See the [MimirSQL Docs](https://github.com/UBOdin/mimir/wiki/Mimir-SQL) for more details, as well as the [Lens and Adaptive Schema Docs](https://github.com/UBOdin/mimir/wiki/Lenses-and-Adaptive-Schemas) for more information about Mimir's data cleaning components.

* Our [Whitepaper Example](http://mimirdb.info/whitepaper.html)
* [MimirSQL Docs](https://github.com/UBOdin/mimir/wiki/Mimir-SQL)
* [Lens and Adaptive Schema Docs](https://github.com/UBOdin/mimir/wiki/Lenses-and-Adaptive-Schemas)
* [Mimir Command-Line Docs](https://github.com/UBOdin/mimir/wiki/Mimir-CLI)


## Compiling Mimir

To compile from source, check out Mimir, and use one of the following to compile and run mimir.
```
$> git clone https://github.com/UBOdin/mimir.git
...
$> cd mimir
$> sbt run
```
OR
```
$> sbt assembly
...
$> ./bin/mimir
```

## Hacking on Mimir

* See the [developer guidelines](https://github.com/UBOdin/mimir/wiki/Development)
* Also see the [ScalaDoc](http://doc.odin.cse.buffalo.edu/mimir)
