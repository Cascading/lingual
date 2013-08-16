#!/usr/bin/env bash
# Copyright 2011-2013 Concurrent, Inc.
# if no args specified, show usage

function show_usage {
  echo "Usage: lingual COMMAND [options]"
  echo "where COMMAND is one of:"
  echo "  shell                execute interactive SQL queries"
  echo "  catalog              manage table and schema catalog"
  echo "  selfupdate           fetch the latest version of lingual"
  echo ""
  echo "Most commands print help when invoked w/o parameters."
  echo ""
  echo "To set a default platform, set the LINGUAL_PLATFORM env variable."
}

if [ $# = 0 ]; then
  show_usage
  exit 1
fi

# get arguments
COMMAND=$1
shift
# figure out which class to run
if [ "$COMMAND" = "shell" ] ; then
  MAIN=cascading.lingual.shell.Shell
elif [ "$COMMAND" = "catalog" ] ; then
  MAIN=cascading.lingual.catalog.Catalog
elif [ "$COMMAND" = "selfupdate" ] ; then
  curl http://@location@/lingual/@majorVersion@/lingual-client/install-lingual-client.sh | bash
  exit $?
else
  echo "ERROR: unknown command: $COMMAND"
  show_usage
  exit 1
fi

# find the dir this is in, regardless of symlinkage. Modified from http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  BASE_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="BASE_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
BASE_DIR="$( cd -P "$( dirname "$SOURCE" )/../" && pwd )"
BIN_DIR="$BASE_DIR/bin"
JAVA_EXEC=`which java`
PLATFORM=${LINGUAL_PLATFORM:-local}
OPTIONS=

ARGS=("$@")

while [ -n "$1" ]
 do
     case $1 in
         --platform)
             PLATFORM=$2
             shift 2
             ;;
         --debug)
             OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 ${OPTIONS}"
             shift
             ;;
         *)  # no more options. Stop while loop
             shift
             ;;
     esac
 done

LINGUAL_CLASSPATH="$BASE_DIR/lib/*:$BASE_DIR/platform/$PLATFORM/*:$BASE_DIR/bin/*"

case $PLATFORM in
   local)
       ;;
   hadoop)
       HADOOP_CLASSPATH=
       source $BIN_DIR/hadoop-env
       LINGUAL_CLASSPATH="$LINGUAL_CLASSPATH:$HADOOP_CLASSPATH"
       ;;
   *)
       echo "ERROR: Unknown platform: $PLATFORM"
       exit 1
       ;;
esac

LINGUAL_BIN_DIR=$BIN_DIR
LINGUAL_BASE_DIR=$BASE_DIR

export LINGUAL_BIN_DIR
export LINGUAL_BASE_DIR
${JAVA_EXEC} -Xmx512m $OPTIONS -cp "$LIBS:$LINGUAL_CLASSPATH" $MAIN "${ARGS[@]}"