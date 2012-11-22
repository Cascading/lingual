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

| context             | action                       | description
| ------------------- | ---------------------        | -----------
| --uri [uri]         |                              | optional path to the catalog meta-data, defaults to current directory
|                     |                              |
| --platform          |                              | lists all known platforms
| --platform [name]   |                              | use the named platform (relative uri will be resolved for given platform)
| --default           |                              | make the current relevant options the default environment
|                     |                              |
| --repo              |                              | list all maven repos
| --repo [name]       |                              |
|                     | --add [uri]                  | add maven repo
|                     | --remove                     | remove maven repo
|                     |                              |
| --init              |                              | initializes a new catalog in the current directory if --uri is not given
| --schema            |                              | lists all current schemas
| --schema [name]     |                              |
|                     | --add [uri]*                 | uri optional, add path as a new schema root
|                     | --remove                     |
|                     | --rename [new name]          |
|                     |                              |
| --table             |                              | lists all tables for the current schema
| --table [name]      |                              |
|                     | --remove                     | logically removes table, does not delete files
|                     | --rename [new name]          | logically renames table, does not alter files
|                     | --add [uri]                  | add path as a new table root, will attempt to resolve stereotype
|                     | --stereotype [name]          | use existing stereotype for table definition
|                     | --format [name]              | use format for identifier
|                     | --protocol [name]            | optional, use protocol for identifier
|                     | --show                       | display table details
|                     |                              |
| --stereotype        |                              | list all registered stereotype names
| --stereotype [name] |                              |
|                     | --remove                     |
|                     | --rename [new name]          |
|                     | --add [uri]*                 | uri optional, read uri for definition or use following values
|                     | --update                     | update with given values (replaces values)
|                     | --columns [names,.]          |
|                     | --types [types,.]            |
|                     | --providers [name,.]         |
|                     | --show                       | display stereotype details
|                     |                              |
| --provider          |                              | list all registered protocol and format providers
| --provider [name]   |                              | regsiter a new provider
|                     | --remove                     |
|                     | --rename [new name]          |
|                     | --add                        | create a new provider with the following values
|                     | --scheme [class]             | Scheme class name
|                     | --scheme-param [key=value,.] | constructor parameters for class
|                     | --tap [class]                | Tap class name
|                     | --tap-param [key=value,.]    | constructor parameters for class
|                     | --format [name]              | format, like CSV, TSV, Avro, or Thrift
|                     | --protocol [name]            | optional, will use default for scheme
|                     | --jar [uri]                  | jar path containing Tap/Scheme provider classes
|                     | --dependency [spec]          | maven dependency, group:name:version
|                     |                              |
| --format            |                              | list all registered format names
| --format [name]     |                              |
|                     | --remove                     |
|                     | --rename [new name]          |
|                     | --add                        | regsiter a new format, like CSV, TSV, Avro, or Thrift
|                     | --ext [.ext,.]               | file extension used to identify format (.csv, .tsv, etc)
|                     |                              |
| --protocol          |                              | list all registered protocol names
| --protocol [name]   |                              |
|                     | --remove                     |
|                     | --rename [new name]          |
|                     | --add                        | register a new protocol
|                     | --scheme [uri,.]             | uri scheme to identify protocol (jdbc:, hdfs:, etc)


# Catalog Structure

Any directory can be the root namespace for a catalog

| path         | description
|------------- |-----------------
| .            | current directory
| ./.lingual/  | all meta-data (hidden directory)
|   defaults   | default environment values
|   catalog    | catalog data file
|   jars/      | copies of scheme jar files
| ./results    | local storage for all SELECT query results sets

