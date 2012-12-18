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

LIBS="$BASEDIR/lib/*"

# get arguments
COMMAND=$1
shift
# figure out which class to run
if [ "$COMMAND" = "shell" ] ; then
  MAIN=cascading.lingual.shell.Shell
elif [ "$COMMAND" = "catalog" ] ; then
  MAIN=cascading.lingual.catalog.Catalog
else
  echo "ERROR: unknown command: $COMMAND"
  exit 1
fi

PLATFORM=local

ARGS="$@"

while [ -n "$1" ]
 do
     case $1 in
         --platform)
             PLATFORM=$2
             shift 2
             ;;
         *)  # no more options. Stop while loop
             shift
             ;;
     esac
 done

OPTIONS=

LINGUAL_CLASSPATH="$BASEDIR/platform/$PLATFORM/*"

case $PLATFORM in
   local)
       ;;
   hadoop)
       HADOOP_CLASSPATH=
       [ -n "$HADOOP_HOME" ] && . $BINDIR/hadoop-env
       LINGUAL_CLASSPATH="$LINGUAL_CLASSPATH:$HADOOP_CLASSPATH"
       ;;
   *)
       echo "ERROR: Unknown platform: $PLATFORM"
       exit 1
       ;;
esac

#[ -n "$COLUMNS" ] && OPTIONS="--display-width $COLUMNS $OPTIONS"

$JAVA_EXEC -Xmx512m $OPTIONS -cp "$LIBS:$LINGUAL_CLASSPATH" $MAIN $ARGS