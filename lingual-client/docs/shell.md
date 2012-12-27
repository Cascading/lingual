# Overview

Lingual Shell is an interactive SQL command shell based on [SQLLine](http://sqlline.sourceforge.net).

Lingual Shell expects to find a catalog configuration that defines available Schemas and Tables.

For more information, visit [Cascading Lingual](http://cascading.org/lingual).

# CLI Usage

Catalog is invoked from the command line via:

    lingual shell [switches]*

To start the shell for running queries on Apache Hadoop:

    lingual shell --platform hadoop

# CLI Options Reference

| switch              | description
| ------------------- | -----------
|                     |
| --platform [name]   | use the named platform
|                     |
| --schema [name]     | name of the default schema (same as `set schema _name_`)
|                     |
| --schemas [uri,...] | root path for each schema to use, will use directory as schema name
|                     |
| --sql [file]        | file with SQL commands to execute
|                     |
| --resultPath [dir]  | where to store temporary result sets
| --dotPath [dir]     | where to write out the Cascading planner DOT file for debugging
|                     |

# Configuration

See [Configuring Apache Hadoop](hadoop.md) for using with a Apache Hadoop cluster.