#!/usr/bin/env bash

BASIC_FILE="basic.sh"

pushd ../../

[ ! -f "$BASIC_FILE" ] && echo "$BASIC_FILE does not exist!" && exit -1

source $BASIC_FILE 
setpath
ldfunc
SLAVES_SIZE=`wc -l conf/slaves | awk '{print $1}'`

popd

create_tera(){
  P=$1
  S_DIR="${P}G-tera"

  create_tera_data "${S_DIR}" "$((P*4/SLAVES_SIZE))" "256"
}


create_text(){
  P=$1
  S_DIR="${P}G-text"

  create_text_data "${S_DIR}" "$((P*4/SLAVES_SIZE))" "256"
}

create_text 20
create_text 40
create_text 80
create_tera 20
create_tera 40
create_tera 80
#create_tera 80
