#!/bin/sh

if [ -z "${HADOOP_HOME}" ]; then
  echo "warning: HADOOP_HOME is not set."
fi

if [ -n "${HADOOP_CONF_DIR}" ]; then
  if [ ! -d "${HADOOP_CONF_DIR}" ]; then
    echo "warning: HADOOP_CONF_DIR is set, but does not exist: ${HADOOP_CONF_DIR}"
  else
    HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}
  fi
elif [ -d ${HADOOP_HOME}/conf ]; then
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${HADOOP_HOME}/conf
fi

for f in ${HADOOP_HOME}/hadoop-core-*.jar; do
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done

HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HADOOP_HOME}/lib/*"