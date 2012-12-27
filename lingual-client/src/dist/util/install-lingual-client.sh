#!/bin/bash
set -e -x

# Usage:
#  --user-home - an alternative user to install into, default /home/hadoop
#  --tmpdir - an alternative temporary directory, default TMPDIR or /tmp if not set
#  --no-bash - do not update .bashrc

LATEST=http://@location@/lingual/@majorVersion@/client/latest.txt

INSTALL_PATH="/usr/local"

case "`uname`" in
  Darwin)
    USER_HOME=/Users/$USER;;
  *)
    USER_HOME=/home/$USER;;
esac

BASH_PROFILE=.bashrc
UPDATE_BASH=y

[ -n $TMPDIR ] && TMPDIR=/tmp

error_msg () # msg
{
  echo 1>&2 "Error: $1"
}

error_exit () # <msg> <cod>
{
  error_msg "$1"
  exit ${2:-1}
}

while [ $# -gt 0 ]
do
  case "$1" in
    --user-home)
      shift
      USER_HOME=$1
      ;;
    --tmpdir)
      shift
      TMPDIR=$1
      ;;
    --no-bash)
      UPDATE_BASH=
      ;;
    -*)
      # do not exit out, just note failure
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
  esac
  shift
done

LINGUAL_HOME=$INSTALL_PATH/lingual

#if DRIVEN_HOME does not exist, create it
if [ ! -d "$LINGUAL_HOME" ]; then
    sudo mkdir -p $LINGUAL_HOME
    sudo chmod a+r $LINGUAL_HOME
fi

REDIR=$TMPDIR/latest
ARCHIVE=$TMPDIR/archive.tgz

# find latest driven client
wget -S -T 10 -t 5 $LATEST -O $REDIR

# download latest
wget -S -T 10 -t 5 -i $REDIR -O $ARCHIVE

# unpack into /usr/local/<driven home>
sudo tar -xzf $ARCHIVE -C $LINGUAL_HOME

sudo chmod a+x $LINGUAL_HOME/bin/*

if [ -n "$UPDATE_BASH" -a -w "$USER_HOME/$BASH_PROFILE" ]; then
cat >> $USER_HOME/$BASH_PROFILE <<- EOF

# Lingual - Concurrent, Inc.
# http://www.concurrentinc.com/

export LINGUAL_HOME=$LINGUAL_HOME

# add driven tool to PATH
export PATH=\$PATH:\$LINGUAL_HOME/bin

EOF
fi
