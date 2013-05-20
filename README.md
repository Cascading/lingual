# Overview

[Lingual](http://www.cascading.org/lingual/) is __true__ SQL for Cascading and Apache Hadoop.

Lingual includes JDBC Drivers, SQL command shell, and a catalog manager for creating schemas and tables.

Lingual is still under active development under the wip-1.0 branch. Thus all wip releases are made available
from the `files.concurrentinc.com` domain. When Lingual hits 1.0 and beyond, final releases will be under
`files.cascading.org`.

To use Lingual, there is no installation other than the optional command line utilities.

Lingual is based on the [Cascading](http://cascading.org) distributed processing engine and
the [Optiq](https://github.com/julianhyde/optiq) SQL parser and rule engine.

See `lingual-client/README.md` for details on using the included command line utilities.

Lingual only supports `SELECT` and `INSERT INTO` statements, for example

    `select count( distinct city ), sum( distinct age ) from sales.emps`

    `insert into test.results select empno, name from sales.emps`

DDL statements like `CREATE TABLE` are unsupported at this time. See the Lingual Catalog tool for defining Schemas
and Tables from the command line.

For more information, visit: http://www.cascading.org/lingual/

# Installing the Lingual Shell and Catalog

To install the Lingual command line tools, call:

    > curl http://files.concurrentinc.com/lingual/1.0/lingual-client/install-lingual-client.sh | bash

This scripts downloads and installs the latest `lingual` shell script into `~/.lingual-client/` and updates any
local `.bashrc` file.

To get the latest release, call:

    > lingual selfupdate

You can optionally bypass the installation and just download the latest version of the Lingual client by calling:

    > wget -i http://files.concurrentinc.com/lingual/1.0/lingual-client/latest.txt

Note, the `install-lingual-client.sh` file is also a valid Amazon EMR bootstrap action.

    elastic-mapreduce \
      --create \
      --instance-group master --instance-count 1 --instance-type $MASTER_INSTANCE_TYPE \
      --instance-group core --instance-count $1 --instance-type $SLAVE_INSTANCE_TYPE \
      --bootstrap-action s3://files.cascading.org/sdk/2.1/install-cascading-sdk.sh \
      --bootstrap-action s3://files.concurrentinc.com/lingual/1.0/lingual-client/install-lingual-client.sh \
      --name "Cascading Cluster - $USER" \
      --key-pair $EMR_SSH_KEY_NAME \
      --alive

# Using the JDBC Drivers

Lingual provides two JDBC Driver jars with self contained dependencies in the [Conjars](http://conjars.org) Maven
repository.

The JDBC connection string is of the form `jdbc:lingual:[platform]`, where `[platform]` is either `local` or `hadoop`.

Additional JDBC properties may be set using the form:

    jdbc:lingual:[platform];[property]=[value];[property]=[value];...

Where the property keys are:

 * catalog=[path] - the working directory where your .lingual workspace is kept
 * schema=[name] - set the default schema name to use
 * schemas=[path,path] - URI paths to the set of schema/tables to install in the catalog
 * resultPath=[path] - temporary root path for result sets to be stored
 * flowPlanPath=[path] - for debugging, print the corresponding Flow dot file here
 * sqlPlanPath=[path] - for debugging, print the corresponding SQL plan file here

Platform is requried to help the driver distinguish between either backend when more than one JDBC Driver is in
the CLASSPATH.

To pull either of these jars, the `jdbc` Maven classifier must be used.

For the `local` mode platform:

    <dependency>
      <groupId>cascading</groupId>
      <artifactId>lingual-local</artifactId>
      <version>x.y.z</version>
      <classifier>jdbc</classifier>
    </dependency>

For the `hadoop` mode platform:

    <dependency>
      <groupId>cascading</groupId>
      <artifactId>lingual-hadoop</artifactId>
      <version>x.y.z</version>
      <classifier>jdbc</classifier>
    </dependency>

Alternatively, pulling the default artifacts (without the classifier) will also pull any relevant dependencies as
would be expected.

# Reporting Issues

The best way to report an issue is to add a new test to `SimpleSqlPlatformTest` along with the expected result set
and submit a pull request on GitHub.

Failing that, feel free to open an [issue](https://github.com/Cascading/lingual/issues) on the [Cascading/Ligual](https://github.com/Cascading/lingual)
project site or mail the [mailing list](https://groups.google.com/forum/?fromgroups#!forum/lingual-user).

# Developing

Running:

    > gradle idea

from the root of the project will create all IntelliJ project and module files, and retrieve all dependencies.

