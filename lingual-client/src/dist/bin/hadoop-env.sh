HADOOP_EXEC=`which hadoop`

if [ -z ${HADOOP_EXEC} ]; then
  if [ ! -z "$HADOOP_CONF_DIR" -a -f $HADOOP_CONF_DIR/../bin/hadoop ]; then
    HADOOP_EXEC=$HADOOP_CONF_DIR/../bin/hadoop
  elif [ ! -z "$HADOOP_HOME" -a -f $HADOOP_HOME/bin/hadoop ] ; then
    HADOOP_EXEC=$HADOOP_HOME/bin/hadoop
  fi
fi

if [ -z ${HADOOP_EXEC} ]; then
    echo "Could not find HADOOP_HOME, HADOOP_CONF_DIR, nor is 'hadoop' in the PATH. exiting."
    exit 1
else
   HADOOP_CLASSPATH=`${HADOOP_EXEC} classpath`
fi
