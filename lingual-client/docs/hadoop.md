# Using with Apache Hadoop

When using with Apache Hadoop, the Shell expects the following environment variable so that the correct Hadoop
version and configuration may be includedin the CLASSPATH.

 * `HADOOP_HOME` - path to local Hadoop installation
 * `HADOOP_CONF_DIR` - defaults to `$HADOOP_HOME/conf`
 * `HADOOP_USER_NAME` - the username to use when submitting Hadoop jobs

## AWS EMR

If working with a remote Amazon Elastic MapReduce cluster, see the [Bash EMR](https://github.com/cwensel/bash-emr)
utilities, specifically the `emrconf` command to fetch remote configuration files.
