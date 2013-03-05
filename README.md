# Overview

Lingual is __true__ SQL for Cascading and Apache Hadoop.

Lingual includes a JDBC Driver, SQL command shell, and a catalog manager for creating schemas and tables.

To use Lingual, there is no installation other than the optional command line utilities.

Lingual is based on the [Cascading](http://cascading.org) distributed processing engine and
the [Optiq](https://github.com/julianhyde/optiq) SQL parser and rule engine.

See `lingual-client/docs` for details on using the included command line utilities.

Lingual only supports `SELECT` and `INSERT INTO` statements, for example

    `select count( distinct city ), sum( distinct age ) from sales.emps`

    `insert into test.results select empno, name from sales.emps`

DDL statements like `CREATE TABLE` are unsupported. See the Lingual Catalog tool for defining Schemas and Tables from
the command line.

# Installing

To install the Lingual command line tools, call:

    > curl http://files.concurrentinc.com/lingual/1.0/lingual-client/install-lingual-client.sh | bash

This scripts downloads and installs the latest `lingual` shell script into `~/.lingual-client/` and updates any
local `.bashrc` file.

To get the latest release, call:

    > lingual selfupdate

# Developing

Running:

    > gradle idea

from the root of the project will create all IntelliJ project and module files, and retrieve all dependencies.

