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

