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
MIMIR_LENSES   R            S          
```

Mimir uses an auxilliary table to track persistent state called MIMIR_LENSES.  The Sqlite database includes two tables: R(A,B,C) and S(B,D).
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
Initializing... done

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
The output is identical to Sqlite, and indeed the queries are being passed through almost directly.  Mimir allows you to define views that apply one or more lenses called interpretive views, or iviews.  We've defined one such iview in `debug.db` already called `sane_r` that hides the fact that you've got null values in R's B attribute.
```
mimir> select * from sane_r;
A,B,C
------
1,2,3
1,3,1
2,3*,1
1,2,NULL
1,4,2
2,2,1
4,2,4
```
Note that the null value has vanished.  We've used a machine learning model to pick a likely replacement.  However, also note the asterisk next to that result -- the value we picked may be erroneous, so Mimir warns you not to trust that particular value completely.  Let's try that again with a slightly more complex query
```
mimir> select * from SANE_R where b = 3;
A,B,C
------
1,2,3
2,3*,1 (This row may be invalid)
1,2,NULL
2,2,1
4,2,4
```
Whoops!  B is uncertain.  This is *probably* the output, but we can't be certain.  Mimir warns you.  How about the opposite case?
```
mimir> select * from SANE_R where b <> 3;
A,B,C
------
1,3,1
1,4,2
( There may be missing result rows )
```
Once again, we can't be certain about the output.  Mimir warns you!  Note, by the way that we're working with pure SQL.  Other than the fact that we're querying a lens relation, there's nothing special about the queries that we're asking.  The only difference comes up in the UI.  Let's try one last thing: 
```
mimir> select a, d from SANE_R r, s where r.b = s.b;
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
mimir> explain select * from SANE_R;
--- Raw Query ---
Project[A <= SANE_R_A, B <= SANE_R_B, C <= SANE_R_C]
  Project[SANE_R_A <= A, SANE_R_B <= B, SANE_R_C <= C]
    Project[A <= A, B <= CASE WHEN B IS NULL THEN {{ SANE_R_0[ROWID] }} ELSE B END, C <= C]
      Project[A <= R_A, B <= R_B, C <= R_C]
        Table(R => R_A, R_B, R_C)
--- Optimized Query ---
Project[A <= R_A, B <= CASE WHEN R_B IS NULL THEN {{ SANE_R_0[ROWID] }} ELSE R_B END, C <= R_C]
  Table(R => R_A, R_B, R_C // ROWID)
```
There are a few things happening here.  Let's look at the raw query first.  The innermost projection is the same renaming projection that was in the first query, and indeed the view definition is based on the same `SELECT * FROM R`.  The two outermost projections carry out a similar renaming given by the query -- bookkeeping at the view and query boundaries to make sure all of the attribute names line up.  The remaining (third) projection is where all of the action takes place.  You'll note that the B attribute is being rewritten by a `CASE` expression.  If B isn't null, nothing special happens.  If B is null however, it is replaced by this odd expression `{{ SANE_R_0[ROWID] }}`.  

This expression represents a probabilistic variable!  Specifically this variable corresponds to the value that the missing value lense sticks in to replace any missing value in the source data.  We instantiate one copy of the variable for every row (Hence the parameter [ROWID]), but ignore most of them since they don't affect the output.

You'll note in the optimized query, all of the projections get folded up.  But this time, something's different.  The outermost projection gets evaluated in Mimir rather than the backend database.  This allows us to track when a value is uncertain, and tag outputs accordingly.  Let's see what happens in one other case
```
mimir> explain select * from SANE_R where b = 3;
--- Raw Query ---
Project[A <= SANE_R_A, B <= SANE_R_B, C <= SANE_R_C]
  Select[ (SANE_R_B=3) ]
    Project[SANE_R_A <= A, SANE_R_B <= B, SANE_R_C <= C]
      Project[A <= A, B <= CASE WHEN B IS NULL THEN {{ SANE_R_0_0[ROWID] }} ELSE B END, C <= C]
        Project[A <= R_A, B <= R_B, C <= R_C]
          Table(R => R_A, R_B, R_C)
--- Optimized Query ---
Project[A <= R_A, B <= CASE WHEN R_B IS NULL THEN {{ SANE_R_0_0[ROWID] }} ELSE R_B END, C <= R_C, __MIMIR_CONDITION <=  (CASE WHEN R_B IS NULL THEN {{ SANE_R_0_0[ROWID] }} ELSE R_B END=3) ]
  Table(R => R_A, R_B, R_C // ROWID)
```
The explain output looks quite similar to before.  The selection operator comes up in the initial stack of operators, but otherwise the raw query is identical.  Note however, that it's gone in the optimized query.  This is because the selection predicate depends on an uncertain attribute (B).  Because of this, we evaluate the condition in-line in Mimir.  The projection tracks a special column called `__MIMIR_CONDITION` that identifies the expression we can use to decide whether a row is in the result set.

