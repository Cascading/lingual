#!/bin/sh

if [ "$HADOOP_HOME" = "" ]; then
  echo "warning: HADOOP_HOME is not set."
fi

for f in $HADOOP_HOME/hadoop-core-*.jar; do
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
done

# add libs to CLASSPATH
#for f in $HADOOP_HOME/lib/*.jar; do
#  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
#done

HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HADOOP_HOME}/lib/*";
