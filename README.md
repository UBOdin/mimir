# The Mimir Uncertainty Management Tool

One of the biggest costs in analytics is data wrangling: getting your messy, mis-labeled, disorganized data into a format that you can actually ask questions about. Unfortunately, most tools for data wrangling force you to do all of this work upfront — before you actually know how much wrangling you really need to do.

Mimir is about getting you to your data analytics as fast as possible. It attaches to a database of your choice using JDBC and provides a suite of lightweight, easy-to-use data cleaning and data analysis tools.

Instead of forcing you to spend hours or days wrangling all your data upfront, Mimir makes educated guesses to get your wrangling done sooner, and give you acess to your data. Then, when you start your analysis, Mimir uses simple visual cues to let you know when your analysis might need you to actually do some data wrangling and how badly it needs it. Then, Mimir streamlines the process of after-the-fact wrangling, focusing you on those parts of the data that need it most.

## Getting Started with Mimir

We don't have an official release yet.  For now, check out the repository and use sbt run to start the Mimir UI
```
$> git clone https://github.com/UBOdin/mimir-ui.git
...
$> cd mimir; sbt run
...
```

Once the Mimir graphical user interface server starts up, just point your web browser at `http://localhost:9000`

Then, you can follow along with the example in our [Whitepaper](http://mimirdb.info/whitepaper.html)


## Hacking on Mimir

* See the [developer guidelines](https://github.com/UBOdin/mimir/wiki/Development)
* Also see the [ScalaDoc](http://doc.odin.cse.buffalo.edu/mimir)
