# Overview

The Lingual Catalog command line tool allows users to curate a catalog of database schemas and tables, where a table is
a Tap accessible dataset and a schema is a collection of tables.

The Lingual Catalog is used in tandem with the Lingual Shell and JDBC drivers  By default the shell will use the current
catalog of schemas and tables to satisfy the SQL planner.

These concepts are inherited from the database world and are compatible with standard SQL tools.

# CLI Usage

Catalog is invoked from the command line via.

    lingual catalog [switches]*

To create a new catalog in the user root directory on HDFS:

    --platform hadoop --init

To add new table to an existing schema:

    --platform hadoop --schema company --table employees --add ./data/employees

# CLI Options Reference

| switch            | action              | description
| ----------------- | ------------------- | -----------
| --platform        |                     | lists all known platforms
| --platform [name] |                     | use the named platform
| --uri [uri]       |                     | optional path to the catalog meta-data
| --default         |                     | make the prior options the default environment
|                   |                     |
| --init            |                     | initializes a new catalog in the current directory if --path is not given
| --schema          |                     | lists all current schemas
| --schema [name]   |                     |
|                   | --add [uri]         | add path as a new schema root
|                   | --remove            |
|                   | --rename [new name] |
| --table           |                     | lists all tables for the current schema
| --table [name]    |                     |
|                   | --add [uri]         | add path as a new table root
|                   | --remove            |
|                   | --rename [new name] |
|                   | --columns [names]*  |
|                   | --types [types]*    |
|                   | --format [name]     |
|                   | --show              | display table details
|                   |                     |
| --format          |                     | list all registered formats
| --format [name]   |                     |
|                   | --add [class]       |
|                   | --param [key=value]*| constructor parameters for class
|                   | --show              |


# Catalog Structure

Any directory can be the root namespace for a catalog

| path     | description
|----------|-----------------
| .        | current directory
| lingual  | all meta-data
| results  | temporary storage for all SELECT query results sets

