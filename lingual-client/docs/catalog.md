# Overview

The Lingual Catalog command line tool allows users to curate a catalog of database schemas and tables, where a table is
a Tap accessible data-set and a schema is a collection of tables.

The Lingual Catalog is used in tandem with the Lingual Shell and JDBC drivers. By default the Shell will use the current
catalog of schemas and tables to satisfy the SQL planner.

These concepts are inherited from the database world and are compatible with standard SQL tools.

# Detail

A Schema is a collection of Tables or nested Schemas. A Schema has a name like _employees_.

A Table consists of a URI, Stereotype, a Format, and a Protocol. A Table also has a name like _titles_. The full name, if the Table
belonged to the Schema _employees_ would be _employees.titles_.

Format is the format or encoding of a Table URI. Tab delimited (TSV) and comma delimited (CSV) are common text formats
can be identified by the file extension on the URI (`employees/titles.csv`). Format maps to a Cascading Scheme
instance internally (like `TextDelimited` for CSV).

Protocol is how a Table URI is accessed. If on Hadoop, the default is HDFS. If in Local mode, the default is through
the local filesystem. Protocols can be identified the the URI 'scheme'. `hdfs:/...` for HDFS and `file:/...` for the local
filesystem. Protocol maps to a Cascading Tap type internally (like `HFS` for HDFS).

A Stereotype represents the meta-data associated with a Table, the table definition, which includes column names
and column types. Stereotypes have a name, may be nested in a Schema, and may be shared between Tables.

Use the command line to create and update new Schema, Table, Stereotype, Format, and Protocols.

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
| --platform          |                              | lists all known platforms (currently local and hadoop)
| --platform [name]   |                              | use the named platform (relative uri will be resolved for given platform)
| --default*          |                              | make the current relevant options the default environment
|                     |                              |
| --init              |                              | initializes a new catalog in the current directory if --uri is not given
|                     |                              |
| --ddl [file]        |                              | use DDL file to define tables in an existing schema
|                     | --schema name                |
|                     | --format name                |
|                     | --protocol name              |
|                     |                              |
| --repo*             |                              | list all maven repos
| --repo [name]       |                              |
|                     | --remove                     | remove maven repo
|                     | --add [uri]                  | add maven repo
|                     |                              |
| --schema            |                              | lists all current schemas
| --schema [name]     |                              |
|                     | --remove                     |
|                     | --rename [new name]          |
|                     | --add [uri]                  | uri optional, add path as a new schema root
|                     |                              |
| --table             |                              | lists all tables for the current schema
| --table [name]      |                              |
|                     | --remove                     | logically removes table, does not delete files
|                     | --rename [new name]          | logically renames table, does not alter files
|                     | --add [uri]                  | add path as a new table root, will attempt to resolve stereotype
|                     | --update                     | updates the table with new properties
|                     | --stereotype [name]          | use existing stereotype for table definition
|                     | --format [name]              | use format for uri identifier
|                     | --protocol [name]            | optional, use protocol for uri identifier
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
|                     | --show                       | display stereotype details
|                     |                              |
| --provider*         |                              | list all registered protocol and format providers
| --provider [name]   |                              | regsiter a new provider
|                     | --remove                     |
|                     | --rename [new name]          |
|                     | --add                        | create a new provider with the following values
|                     | --jar [uri]                  | jar path containing Tap/Scheme provider classes
|                     | --dependency [spec]          | maven dependency, group:name:version
|                     |                              |
|                     |                              |
| --format            |                              | list all registered format names
| --format [name]     |                              |
|                     | --remove                     |
|                     | --add                        | regsiter a new format, like CSV, TSV, Avro, or Thrift
|                     | --provider [name]*           | use the given provider
|                     | --update                     | update with given values (replaces values)
|                     | --ext [.ext,.]               | file extension used to identify format (.csv, .tsv, etc)
|                     | --properties [name=value,.]  | update/add properties for the format (hasHeaders=true, etc)
|                     |                              |
| --protocol          |                              | list all registered protocol names
| --protocol [name]   |                              |
|                     | --remove                     |
|                     | --add                        | register a new protocol
|                     | --provider [name]*           | use the given provider
|                     | --update                     | update with given values (replaces values)
|                     | --uris [uri,.]               | uri scheme to identify protocol (jdbc:, hdfs:, etc)
|                     | --properties [name=value,.]  | update/add properties for the protocol (user=jsmith, etc)

__* currently unsupported__

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

# Configuration

See [Configuring Apache Hadoop](hadoop.md) for using with a Apache Hadoop cluster.
