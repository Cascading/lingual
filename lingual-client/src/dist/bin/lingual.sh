#!/usr/bin/env bash
# Copyright 2011-2012 Concurrent, Inc.

BINDIR=`dirname $0`
BASEDIR=`dirname ${BINDIR}`

if [ "$1" = "selfupdate" ]; then
  curl http://@location@/lingual/@majorVersion@/client/install-lingual-client.sh | sh
  exit 0
fi

# if no args specified, show usage
if [ $# = 0 ]; then
  echo "Usage: lingual COMMAND [options]"
  echo "where COMMAND is one of:"
  echo "  shell                configure/manage the server"
  echo "  catalog              manage table and schema catalog"
  echo "  selfupdate           fetch the latest version of driven"
  echo "Most commands print help when invoked w/o parameters."
  exit 1
fi

JAVA_EXEC=`which java`
[ -n "$JAVA_HOME" ] && JAVA_EXEC=$JAVA_HOME/bin/java

[ ! -e $JAVA_EXEC ] && echo "could not find java, check JAVA_HOME!!" && exit -1

LIBS=$BASEDIR/lib

# get arguments
COMMAND=$1
shift
# figure out which class to run
if [ "$COMMAND" = "shell" ] ; then
  MAIN=cascading.lingual.shell.Shell
elif [ "$COMMAND" = "catalog" ] ; then
  MAIN=cascading.lingual.catalog.Catalog
fi

OPTIONS=

HADOOP_CLASSPATH=

[ -n "$HADOOP_HOME" ] && . $BINDIR/hadoop-env

#[ -n "$COLUMNS" ] && OPTIONS="--display-width $COLUMNS $OPTIONS"

$JAVA_EXEC -Xmx512m -cp "$LIBS/*:$HADOOP_CLASSPATH" $MAIN $OPTIONS "$@"