# TeraSort benchmark for Spark

[![Build
Status](https://travis-ci.org/ehiggs/spark-terasort.svg)](https://travis-ci.org/ehiggs/spark-terasort)

This is an example Spark program for running TeraSort benchmarks. It is based on
work from [Reynold Xin's branch](https://github.com/rxin/spark/tree/terasort),
but it is not the same TeraSort program that currently holds the
[record](http://sortbenchmark.org/). That program is
[here](https://github.com/rxin/spark/tree/sort-benchmark/core/src/main/scala/org/apache/spark/sort).

# Building

`mvn install`

The default is to link against Spark 2.1 jars (released December 2016). If you plan to run using an older version of Spark (e.g. 1.6) you will have to try `-Dspark.version=1.6`. If possible, it's probably a better idea to just update to a more recent version or Spark.

# Running

`cd` to your your Spark install.

## Generate data

    ./bin/spark-submit --class com.github.ehiggs.spark.terasort.TeraGen 
    path/to/spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 
    1g file://$HOME/data/terasort_in 

## Sort the data
    ./bin/spark-submit --class com.github.ehiggs.spark.terasort.TeraSort
    path/to/spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 
    file://$HOME/data/terasort_in file://$HOME/data/terasort_out

## Validate the data
    ./bin/spark-submit --class com.github.ehiggs.spark.terasort.TeraValidate
    path/to/spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 
    file://$HOME/data/terasort_out file://$HOME/data/terasort_validate

# Known issues

## Performance

This terasort doesn't use the partitioning scheme that Hadoop's Terasort uses.
This results in not very good performance. I could copy the Partitioning code
from the Hadoop tree verbatim but I thought it would be more appropriate to
rewrite more of it in Scala.

I haven't pulled the DaytonaPartitioner from the record holding sort yet because
it's pretty intertwined into the rest of the code and AFAIK it's not really
idiomatic Spark.

## Functionality on native file systems

TeraValidate can read the file parts in the wrong order on native file systems
(e.g. if you run Spark on your laptop, on Lustre, Panasas, etc). HDFS apparently
always returns the files in alphanumeric order so most Hadoop users aren't
affected. I thought I fixed this in the TeraInputFormat, but I was able to
reproduce it since migrating the code from my Spark terasort branch.

# Contributing

PRs are very welcome!
