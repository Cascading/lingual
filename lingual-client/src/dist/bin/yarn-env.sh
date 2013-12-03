if [ -d "${HADOOP_YARN_HOME}" ]; then
  YARN_CLASSPATH=`$HADOOP_YARN_HOME/bin/yarn classpath`
  return
elif [ -d "${HADOOP_HOME}" ]; then
  YARN_CLASSPATH=`$HADOOP_HOME/bin/yarn classpath`
  return
else
  BIN_PATH=`which yarn`
  if [ -n "${BIN_PATH}" ] ; then
    YARN_CLASSPATH=`yarn classpath`
    return
  else
    echo "Could not find HADOOP_HOME, HADOOP_YARN_HOME nor is 'yarn' in the PATH. exiting."
    exit 1
  fi
fi
