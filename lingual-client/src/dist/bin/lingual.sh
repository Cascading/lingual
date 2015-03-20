#!/usr/bin/env bash
# Copyright 2011-2014 Concurrent, Inc.
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

PLATFORM=""
CONFIG=""
CONFIG_FILE=""

CASCADING_CONFIG_FILE=$HOME/.cascading/default.properties
LOCAL_CASCADING_CONFIG_FILE=$PWD/.cascading/default.properties

if [[ -e $LOCAL_CASCADING_CONFIG_FILE ]]; then
  CONFIG_FILE=$LOCAL_CASCADING_CONFIG_FILE
elif [[ -e $CASCADING_CONFIG_FILE ]]; then
  CONFIG_FILE=$CASCADING_CONFIG_FILE
fi

if [[ -n $LINGUAL_PLATFORM ]]; then
  PLATFORM=$LINGUAL_PLATFORM
elif [[ -n $CASCADING_PLATFORM ]]; then
  PLATFORM=$CASCADING_PLATFORM
elif [[ -n $CONFIG_FILE ]]; then
  PLATFORM=`grep '^lingual.platform.name' $CONFIG_FILE | cut -d\= -f2`
  if [[ -z $PLATFORM ]]; then
    PLATFORM=`grep '^cascading.platform.name' $CONFIG_FILE | cut -d\= -f2`
  fi
fi

if [[ -z $PLATFORM ]]; then
    PLATFORM=local
fi

if [[ -n $LINGUAL_CONFIG ]]; then
  CONFIG=$LINGUAL_CONFIG
elif [[ -n $CASCADING_CONFIG ]]; then
  CONFIG=$CASCADING_CONFIG
elif [[ -n $CONFIG_FILE ]]; then
  CONFIG=`grep "^lingual.platform.$PLATFORM.config" $CONFIG_FILE | cut -d\= -f2-`
  if [[ -z $CONFIG ]]; then
    CONFIG=`grep "^cascading.platform.${PLATFORM}.config" $CONFIG_FILE | cut -d\= -f2-`
  fi
fi

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

LINGUAL_CLASSPATH="$LINGUAL_CLASSPATH:$BASE_DIR/lib/*:$BASE_DIR/bin/*"

PLATFORM_CLASSPATH=""

case $PLATFORM in
   local)
       ;;
   hadoop)
       source $BIN_DIR/hadoop-env
       PLATFORM_CLASSPATH="$HADOOP_CLASSPATH"
       ;;
   hadoop2-mr1)
       source $BIN_DIR/yarn-env
       PLATFORM_CLASSPATH="$YARN_CLASSPATH"
       ;;
   hadoop2-tez)
       source $BIN_DIR/yarn-env
       source $BIN_DIR/tez-env
       PLATFORM_CLASSPATH="$YARN_CLASSPATH"
       ;;
   *)
       echo "ERROR: Unknown platform: $PLATFORM"
       exit 1
       ;;
esac
export LINGUAL_CLASSPATH="$LINGUAL_CLASSPATH:$BASE_DIR/platform/$PLATFORM/*:$PLATFORM_CLASSPATH"

OPTIQ_JVM_ARGS=""
for CUR_ARG in "${ARGS[@]}"; do [[ "$CUR_ARG" == "--showstacktrace" ]] && OPTIQ_JVM_ARGS="-Doptiq.debug"; done

OPTIQ_JVM_ARGS="$OPTIQ_JVM_ARGS -Doptiq.container.rel=cascading"
SQLLINE_JVM_ARGS="$SQLLINE_JVM_ARGS -Dsqlline.system.exit=true"
LINGUAL_BIN_DIR=$BIN_DIR
LINGUAL_BASE_DIR=$BASE_DIR

export LINGUAL_BIN_DIR
export LINGUAL_BASE_DIR
export LINGUAL_PLATFORM=$PLATFORM
if [[ -n $CONFIG ]]; then
  export LINGUAL_CONFIG=$CONFIG
fi
${JAVA_EXEC} ${LINGUAL_JVM_OPTS} ${OPTIQ_JVM_ARGS} -Xmx512m ${OPTIONS} -cp "$LINGUAL_CLASSPATH" $MAIN "${ARGS[@]}"
result=$?

# Convert sqlline's error handling back to unix standard
if [ "$result" -eq "255" ] ; then
  exit 1
fi
exit $result
