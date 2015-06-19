#!/bin/bash
#Quick restart script for dev use

set -e

DBNAME="mydb"
SCIDB_INSTALL=/development/scidbtrunk/stage/install

mydir=`dirname $0`
pushd $mydir
make SCIDB=$SCIDB_INSTALL

iquery -aq "unload_library('grouped_aggregate')"
scidb.py stopall $DBNAME 
scidb.py startall $DBNAME
sudo cp libgrouped_aggregate.so $SCIDB_INSTALL/lib/scidb/plugins/
#for multinode setups, dont forget to copy to every instance
iquery -aq "load_library('grouped_aggregate')"

