# The Mimir Uncertainty Management Tool

Mimir wraps around a traditional relational database system and allows users to explicitly track data quality through a construct called a Lens.  Lenses in Mimir are analogous to stages in an ETL pipeline -- responsible for performing data cleaning or analytics tasks.  However, unlike a traditional ETL pipeline, Lenses explicitly track conditions that could compromise data quality: Bulk data copies without considering data volatility, entity-resolution, missing values.  Lenses create the illusion that you're working with high quality data, allowing you to start analyzing the data right away.  Using the power of provenance, Mimir can tell you how reliable your query results are, and what you can do about it.

# Lenses

Lenses are filters based on machine learning techniques that give you the illusion of working with high-quality data until you're ready to deal with the messiness of data cleaning.  

# Building

Build using scala / sbt.  

```
$> sbt package
```

# Demo 1 - What the user sees

Mimir includes a demo sqlite database.  Let's have a look at it.

```
thor:Mimir-Src okennedy$ sqlite3 debug.db 
SQLite version 3.8.5 2014-08-15 22:37:57
Enter ".help" for usage hints.
sqlite> .tables
MIMIR_IVIEW  MIMIR_LENS   R            S          
```

Mimir uses two auxilliary views to track persistent state called MIMIR_IVIEW and MIMIR_LENS.  The Sqlite database includes two tables: R(A,B,C) and S(B,D).
```
sqlite> select * from r;
1|2|3
1|3|1
2||1
1|2|
1|4|2
2|2|1
4|2|4
sqlite> select * from s;
4|3
3|2
4|1
5|2
2|3
4|5
2|4
3|1
1|5
4|5
```
Note that R has several missing values.  Let's start up the Mimir CLI.  
```
thor:Mimir-Src okennedy$ ./bin/mimir 
Loading IViews...
Loading IView: CLEAN_R
Building learner for 'B'
done

mimir> 
```
By default, Mimir connects to the sqlite3 database `debug.db` using JDBC.  Let's start by running some trivial queries.
```
mimir> select * from r;
A,B,C
------
1,2,3
1,3,1
2,NULL,1
1,2,NULL
1,4,2
2,2,1
4,2,4

mimir> select * from s;
B,D
------
4,3
3,2
4,1
5,2
2,3
4,5
2,4
3,1
1,5
4,5
```
The output is identical to Sqlite, and indeed the queries are being passed through almost directly.  Mimir allows you to define views that apply one or more lenses called interpretive views, or iviews.  We've defined one such iview in `debug.db` already called `clean_r` that hides the fact that you've got null values in R's B attribute.
```
mimir> select * from clean_r;
A,B,C
------
1,2,3
1,3,1
2,2*,1
1,2,NULL
1,4,2
2,2,1
4,2,4
```
Note that the null value has vanished.  We've used a machine learning model to pick a likely replacement.  However, also note the asterisk next to that result -- the value we picked may be erroneous, so Mimir warns you not to trust that particular value completely.  Let's try that again with a slightly more complex query
```
mimir> select * from clean_r where b = 2;
A,B,C
------
1,2,3
2,2*,1 (This row may be invalid)
1,2,NULL
2,2,1
4,2,4
```
Whoops!  B is uncertain.  This is *probably* the output, but we can't be certain.  Mimir warns you.  How about the opposite case?
```
mimir> select * from clean_r where b <> 2;
A,B,C
------
1,3,1
1,4,2
( There may be missing result rows )
```
Once again, we can't be certain about the output.  Mimir warns you!  Note, by the way that we're working with pure SQL.  Other than the fact that we're querying a lens relation, there's nothing special about the queries that we're asking.  The only difference comes up in the UI.  Let's try one last thing: 
```
mimir> select a, d from clean_r r, s where r.b = s.b;
A,D
------
1,3
1,4
1,2
1,1
2,3 (This row may be invalid)
2,4 (This row may be invalid)
1,3
1,4
1,3
1,1
1,5
1,5
2,3
2,4
4,3
4,4
( There may be missing result rows )
```
We're working with the data normally, as if it were perfect.  It clearly isn't, but Mimir will tell you exactly when to be concerned, what errors might affect in your output.  

# Demo 2 - Under the Hood

Let's have a look at what's going on under the hood.  Something simple to start:
```
mimir> explain select * from r;         
--- Raw Query ---
Project[A <= R_A, B <= R_B, C <= R_C]
  Table(R => R_A, R_B, R_C)
--- Optimized Query ---
Project[A <= R_A, B <= R_B, C <= R_C]
  Table(R => R_A, R_B, R_C)
```
Nothing fancy.  We scan over the table and then rename the attributes a little.  This query gets punted entirely to the backend database.  Now let's see what happens when we query a lens.
```
mimir> explain select * from clean_r;
--- Raw Query ---
Project[A <= CLEAN_R_A, B <= CLEAN_R_B, C <= CLEAN_R_C]
  Project[CLEAN_R_A <= A, CLEAN_R_B <= B, CLEAN_R_C <= C]
    Project[A <= A, B <= CASE WHEN B IS NULL THEN {{ CLEAN_R_0_0[ROWID] }} ELSE B END, C <= C]
      Project[A <= R_A, B <= R_B, C <= R_C]
        Table(R => R_A, R_B, R_C)
--- Optimized Query ---
Project[A <= R_A, B <= CASE WHEN R_B IS NULL THEN {{ CLEAN_R_0_0[ROWID] }} ELSE R_B END, C <= R_C]
  Table(R => R_A, R_B, R_C // ROWID)
```
There are a few things happening here.  Let's look at the raw query first.  The innermost projection is the same renaming projection that was in the first query, and indeed the view definition is based on the same `SELECT * FROM R`.  The two outermost projections carry out a similar renaming given by the query -- bookkeeping at the view and query boundaries to make sure all of the attribute names line up.  The remaining (third) projection is where all of the action takes place.  You'll note that the B attribute is being rewritten by a `CASE` expression.  If B isn't null, nothing special happens.  If B is null however, it is replaced by this odd expression `{{ CLEAN_R_0_0[ROWID] }}`.  

This expression represents a probabilistic variable!  Specifically this variable corresponds to the value that the missing value lense sticks in to replace any missing value in the source data.  We instantiate one copy of the variable for every row (Hence the parameter [ROWID]), but ignore most of them since they don't affect the output.

You'll note in the optimized query, all of the projections get folded up.  But this time, something's different.  The outermost projection gets evaluated in Mimir rather than the backend database.  This allows us to track when a value is uncertain, and tag outputs accordingly.  Let's see what happens in one other case
```
mimir> explain select * from clean_r where b = 2;
--- Raw Query ---
Project[A <= CLEAN_R_A, B <= CLEAN_R_B, C <= CLEAN_R_C]
  Select[ (CLEAN_R_B=2) ]
    Project[CLEAN_R_A <= A, CLEAN_R_B <= B, CLEAN_R_C <= C]
      Project[A <= A, B <= CASE WHEN B IS NULL THEN {{ CLEAN_R_0_0[ROWID] }} ELSE B END, C <= C]
        Project[A <= R_A, B <= R_B, C <= R_C]
          Table(R => R_A, R_B, R_C)
--- Optimized Query ---
Project[A <= R_A, B <= CASE WHEN R_B IS NULL THEN {{ CLEAN_R_0_0[ROWID] }} ELSE R_B END, C <= R_C, __MIMIR_CONDITION <=  (CASE WHEN R_B IS NULL THEN {{ CLEAN_R_0_0[ROWID] }} ELSE R_B END=2) ]
  Table(R => R_A, R_B, R_C // ROWID)
```
The explain output looks quite similar to before.  The selection operator comes up in the initial stack of operators, but otherwise the raw query is identical.  Note however, that it's gone in the optimized query.  This is because the selection predicate depends on an uncertain attribute (B).  Because of this, we evaluate the condition in-line in Mimir.  The projection tracks a special column called `__MIMIR_CONDITION` that identifies the expression we can use to decide whether a row is in the result set.


# Development

### Compiling the Code

SBT has an interactive mode.  I suggest using this for development purposes.  Start by running `sbt` with no arguments.

```
Thor:Src okennedy$ sbt
[info] Set current project to Mimir (in build file:/Users/okennedy/Documents/Mimir/Src/)
> 
```

Useful SBT commands:
* `compile`: Build all scala and java source files
* `package`: As compile, but also build a jar file 
* `run [args]`: Run mimir from within SBT's interactive mode (you can also use `./bin/mimir`)
* `parser`: Run javacc to build java files for the mimir branch of JSqlParser.
* `test`: Compile and run all unit tests
* `testQuick`: Compile and run all unit tests affected by the most recent set of changes
* `runTest [testName]`: Run a specific test case

SBT also has an auto-run mode that watches the `src` directory for changes and auto-runs the relevant commands.  Invoke auto-run mode by prefixing the command to be executed with a tilde:
```
> ~package
[success] Total time: 0 s, completed May 25, 2015 12:57:32 PM
1. Waiting for source changes... (press enter to interrupt)
[info] Compiling 1 Scala source to /Users/xthemage/Documents/Mimir/Source/target/scala-2.10/classes...
[warn] there were 1 deprecation warning(s); re-run with -deprecation for details
[warn] one warning found
[info] Packaging /Users/xthemage/Documents/Mimir/Source/target/scala-2.10/mimir_2.10-0.1.jar ...
[info] Done packaging.
[success] Total time: 1 s, completed May 25, 2015 12:58:09 PM
2. Waiting for source changes... (press enter to interrupt)
```

### Code Organization

* `src/main/scala/mimir/Mimir.java` : The console UI for Mimir.  The View in the MVC model.
* `src/main/scala/mimir/Database.java` : The central hub of Mimir.  The Controller in the MVC model.
* `src/main/scala/mimir/algebra` : ASTs for Relational Algebra (Operator), and Primitive-Valued Expressions (Expression)
* `src/main/scala/mimir/ctables` : An abstraction layer for representing, rewriting, and analyzing Virtual C-Tables (see also lenses).
* `src/main/scala/mimir/lenses` : An implementation of the lens abstraction: A CTable + One or more Models.
* `src/main/scala/mimir/exec` : Logic for compiling and evaluating queries
* `src/main/scala/mimir/parser` : Logic for parsing text expressions
* `src/main/scala/mimir/sql` : Connectors for interfacing between traditional SQL and Mimir RA
* `src/main/scala/mimir/util` : Random utility classes.
* `src/main/java/mimir` : Legacy and Connector code for machine learning tools and JSqlParser

### Best Practices for Debugging

Debugging involves probing different parts of the code to see how they react.  This is a great opportunity to create test cases that isolate parts of the code that you're having problems with.  As you're debugging code, use [Scala specifications](https://etorreborre.github.io/specs2/) (also see `src/test`) rather than `println()` to trace execution of code fragments.  

1. Specifications force you to think through the expected behavior of the code in question, as you need to define a *correct* output.  
2. After defining a specification, you have a regression test that will prevent similar bugs from arising in the future.

Regardless of how you debug, *Before committing, make sure to run `sbt test`*




