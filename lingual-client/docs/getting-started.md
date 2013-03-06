
# Getting Started

## Basics

The best way to learn Lingual is it download sample data and run a few queries.

After installing Lingual, in a new working directory, download sample data:

    > wget http://data.cascading.org/employees.tgz
    > tar -xzf employees.tgz

Next register the data with lingual:

    > wget http://data.cascading.org/create-employees.sh
    > chmod a+x create-employees.sh
    > ./create-employees.sh

Finall start the Lingual shell, and run a simple query:

    > lingual shell
    > select * from employees.titles;

Here you will see all the first 10,000 recoreds in the `employees.csv` file printed to the screen.

What actually happened under the hood is that a new Cascading Flow was created by the JDBC Driver and run to select
all the `employees` records, which were dumped by default into a file in the `./results/` directory. A JDBC ResultSet
was created to read that file where the "max rows" was set to 10,000.

The file in the `./results/` directory is a valid data file, but should be deleted if you want to reclaim the
space it is taking up.

Note that by default Cascading "local mode" is used, and the `create-employees.sh` script creates a catalog file
in the current workspace under `./.lingual` as `catalog`. `catalog` is a simple human readable JSON file.

## Using Hadoop

To use Lingual against an Apache Hadoop cluster, simply copy your data to hadoop, set HADOOP_HOME (if not already set)
and run `create-employees.sh` again:


    > hadoop fs -copyFromLocal employees employees
    > export HADOOP_HOME=/path/to/hadoop
    > ./create-employees.sh hadoop

Now start the shell in Hadoop mode:

    > lingual shell --platform hadoop
    > select * from employees.titles;

## Advanced

Currently Lingual does not support DDL from the Shell prompt so Catalog must be used to create schemas and tables.

A table must exist in Lingual before an `insert into select ...` statement can be called.

    > lingual catalog --schema working --add

    > lingual catalog --schema working --stereotype titles -add --columns title --types string

    > lingual catalog --schema working --table unique_titles --stereotype titles -add working/unique-titles.csv

    > lingual shell
    > insert into "working"."unique_titles" select distinct( title ) from employees.titles;