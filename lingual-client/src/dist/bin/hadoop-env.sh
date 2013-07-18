#!/bin/sh

if [ -z "${HADOOP_HOME}" ]; then
  echo "warning: HADOOP_HOME is not set. see your hadoop documents for details."
  BIN_PATH=`which hadoop`
  if [ -n "${BIN_PATH}" ] ; then
    HADOOP_HOME=${BIN_PATH%/bin/hadoop}
    echo "lingual will use HADOOP_HOME of ${HADOOP_HOME}"
  else
    echo "exiting."
    exit 1
  fi
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

# hadoop 0.20.205+
for f in ${HADOOP_HOME}/hadoop-core-*.jar; do
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done

# hadoop 0.20.2
for f in ${HADOOP_HOME}/hadoop-*-core.jar; do
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done

HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HADOOP_HOME}/lib/*"
