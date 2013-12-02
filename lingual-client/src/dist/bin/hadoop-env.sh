if [ -z "${HADOOP_HOME}" ]; then
  BIN_PATH=`which hadoop`
  if [ -n "${BIN_PATH}" ] ; then
    HADOOP_CLASSPATH=`hadoop classpath`
    return
  else
    echo "Could not find HADOOP_HOME nor is hadoop in the PATH. exiting."
    exit 1
  fi
else
    HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
fi
