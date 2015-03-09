TeraSort benchmark for Spark

This is an example Spark program for running TeraSort benchmarks. It is based on
work from [Reynold Xin's branch](https://github.com/rxin/spark/tree/terasort),
but it is not the same TeraSort program that currently holds the
[record](http://sortbenchmark.org/). That program is
[here](https://github.com/rxin/spark/tree/sort-benchmark/core/src/main/scala/org/apache/spark/sort).

# Building

`mvn install`

# Running

`cd` to your your spark install.

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
