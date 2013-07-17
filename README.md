# Overview

[Lingual](http://www.cascading.org/lingual/) is __true__ SQL for Cascading and Apache Hadoop.

Lingual includes JDBC Drivers, SQL command shell, and a catalog manager for creating schemas and tables.

Lingual is still under active development under the wip-1.0 branch. Thus all wip releases are made available
from the `files.concurrentinc.com` domain. When Lingual hits 1.0 and beyond, final releases will be under
`files.cascading.org`.

To use Lingual, there is no installation other than the optional command line utilities.

Lingual is based on the [Cascading](http://cascading.org) distributed processing engine and
the [Optiq](https://github.com/julianhyde/optiq) SQL parser and rule engine.

See the [Lingual](http://www.cascading.org/lingual/) page for installation and usage.

# Reporting Issues

The best way to report an issue is to add a new test to `SimpleSqlPlatformTest` along with the expected result set
and submit a pull request on GitHub.

Failing that, feel free to open an [issue](https://github.com/Cascading/lingual/issues) on the [Cascading/Ligual](https://github.com/Cascading/lingual)
project site or mail the [mailing list](https://groups.google.com/forum/?fromgroups#!forum/lingual-user).

# Developing

Running:

    > gradle idea

from the root of the project will create all IntelliJ project and module files, and retrieve all dependencies.

