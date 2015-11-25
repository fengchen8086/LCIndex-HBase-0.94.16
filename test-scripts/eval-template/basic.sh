#!/usr/bin/env bash
# import the basic before each evaluation script for proper settings

# set the basic properties for evaluation

export DEFINE_ONERROR=true
onerror()
{
  local _REASON="$1"
  echo "=== Error ===" | tee -a "$LOG_NAME"
  echo -e "Info: $_REASON." | tee -a "$LOG_NAME"
  echo "[FAIL] MPI-D Test $NB_DATE: $_REASON" 
  exit 1
}


setpath()
{
  _TESTDIR=${1:-`pwd`}
  _TEST_LOG_DIR=${2:-$_TESTDIR/logs}
  NB_DATE=`date +%Y%m%d-%s`
  LOG_NAME="$_TEST_LOG_DIR/app_${NB_DATE}.log"
  REPORT_NAME="$_TEST_LOG_DIR/app_report_${NB_DATE}.log"
  export _TESTDIR LOG_NAME REPORT_NAME
  [ ! -d "$_TEST_LOG_DIR" ] && mkdir -p $_TEST_LOG_DIR
}

ldfunc()
{
  FUNC_DIR="func"
  if [ -d "$FUNC_DIR" ]; then
    for f in `ls $FUNC_DIR`; do
      source "$FUNC_DIR/$f"
    done
  else
    echo "No $FUNC_DIR directory found."
    exit 1
  fi
}

CONFIG_FILE="conf/conf.sh"
if [ -f "$CONFIG_FILE" ]; then
  source "$CONFIG_FILE"
else
  echo "No $CONFIG_FILE found."
  exit 1
fi
