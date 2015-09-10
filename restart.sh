#!/bin/bash
#Quick restart script for dev use

DBNAME="mydb"
SCIDB_INSTALL=~/scidb

iquery -aq "unload_library('grouped_aggregate')"

set -e
mydir=`dirname $0`
pushd $mydir
make SCIDB=$SCIDB_INSTALL SCIDB_THIRDPARTY_PREFIX=/opt/scidb/15.7

scidb.py stopall $DBNAME 
scidb.py startall $DBNAME
sudo cp libgrouped_aggregate.so $SCIDB_INSTALL/lib/scidb/plugins/
#for multinode setups, dont forget to copy to every instance
iquery -aq "load_library('grouped_aggregate')"

