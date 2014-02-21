YARN_EXEC=`which yarn`

if [ -z ${YARN_EXEC} ]; then
  if [ ! -z "$HADOOP_CONF_DIR" -a -f $HADOOP_CONF_DIR/../../bin/yarn ]; then
    YARN_EXEC=$HADOOP_CONF_DIR/../../bin/yarn
  elif [ ! -z "$HADOOP_YARN_HOME" -a -f $HADOOP_YARN_HOME/bin/yarn ] ; then
    YARN_EXEC=$HADOOP_YARN_HOME/bin/yarn
  elif [ ! -z "$HADOOP_HOME" -a -f $HADOOP_HOME/bin/yarn ] ; then
    YARN_EXEC=$HADOOP_HOME/bin/yarn
  fi
fi

if [ -z ${YARN_EXEC} ]; then
    echo "Could not find HADOOP_CONF_DIR, HADOOP_YARN_HOME, HADOOP_HOME nor is 'yarn' in the PATH. exiting."
    exit 1
else
   YARN_CLASSPATH=`${YARN_EXEC} classpath`
fi
