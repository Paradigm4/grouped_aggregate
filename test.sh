#!/bin/bash

MYDIR=$(cd $(dirname $0) && pwd)

# --outfile <filename> : write iquery output to the given file.
# This is important for running in the SciDB test harness.
if [ "$1" = "--outfile" ]; then
    OUTFILE="$2"
    shift 2
else
    OUTFILE=$MYDIR/test.out
fi

rm -f "$OUTFILE"

iquery -anq "remove(foo)" > /dev/null 2>&1
iquery -anq "remove(a_new)" > /dev/null 2>&1
iquery -anq "remove(a_old)" > /dev/null 2>&1
iquery -anq "remove(b_new)" > /dev/null 2>&1
iquery -anq "remove(b_old)" > /dev/null 2>&1
iquery -anq "remove(c_new)" > /dev/null 2>&1
iquery -anq "remove(c_old)" > /dev/null 2>&1
iquery -anq "remove(ac_new)" > /dev/null 2>&1
iquery -anq "remove(ac_old)" > /dev/null 2>&1
iquery -anq "remove(ab_new)" > /dev/null 2>&1
iquery -anq "remove(ab_old)" > /dev/null 2>&1

iquery -anq "
store(
 apply(
  build(<val: double null> [i=1:4000000,500000,0], iif(random()%10=0, null, random() % 100 )),
  a, iif(random() % 2 = 0, 'abc', 'def'),
  b, iif(i % 5 = 0, null, string(i) + '0'),
  c, iif(random() % 10 =0, null, iif(random()%9=0, double(nan), random() % 20 ))
 ),
 foo
)"

iquery -naq "store(grouped_aggregate(foo, a, var(val)), a_new)"
iquery -naq "
store(
 redimension(
  index_lookup(foo as A, uniq(sort(project(foo, a))), A.a, idx),
  <a:string null, val_var: double null> [idx=0:*,1000000,0],
  max(a) as a, var(val)
 ),
 a_old
)"

iquery -naq "store(grouped_aggregate(foo, b, sum(val)), b_new)"
iquery -naq "
store(
 redimension(
  index_lookup(foo as A, uniq(sort(project(foo, b))), A.b, idx),
  <b:string null, val_sum: double null> [idx=0:*,1000000,0],
  max(b) as b, sum(val)
 ),
 b_old
)"

iquery -naq "store(grouped_aggregate(foo, c, avg(val)), c_new)"
iquery -naq "
store(
 redimension(
  index_lookup(foo as A, uniq(sort(project(filter(foo, is_nan(c) = false), c))), A.c, idx),
  <c:double null, val_avg: double null> [idx=0:*,1000000,0],
  max(c) as c, avg(val)
 ),
 c_old
)"

iquery -naq "store(grouped_aggregate(foo, a, c, avg(val)), ac_new)"
iquery -naq "
store(
 redimension(
  index_lookup(
   index_lookup(foo as A, uniq(sort(project(filter(foo, is_nan(c) = false), c))), A.c, cidx),
   uniq(sort(project(foo, a))),
   A.a,
   aidx
  ),
  <a:string null, c:double null, val_avg: double null> [aidx=0:*,1000,0, cidx=0:*,1000,0],
  max(a) as a, max(c) as c, avg(val)
 ),
 ac_old
)"

iquery -naq "store(grouped_aggregate(foo, a, b, max(val), var(val)), ab_new)"
iquery -naq "
store(
 redimension(
  index_lookup(
   index_lookup(foo as A, uniq(sort(project(filter(foo, is_nan(c) = false), a))), A.a, aidx),
   uniq(sort(project(foo, b))),
   A.b,
   bidx
  ),
  <a:string null, b:string null, val_max: double null, val_var:double null> [aidx=0:*,2,0, bidx=0:*,1000000,0],
  max(a) as a, max(b) as b, max(val), var(val)
 ),
 ab_old
)"


iquery -aq "op_count(a_new)" >> $OUTFILE
iquery -aq "op_count(a_old)" >> $OUTFILE
iquery -aq "aggregate(apply(join(sort(a_new,a), sort(a_old,a)), z, iif(a_new.val_var=a_old.val_var, 1,0)), sum(z))" >> $OUTFILE

iquery -aq "op_count(b_new)" >> $OUTFILE
iquery -aq "op_count(b_old)" >> $OUTFILE
iquery -aq "aggregate(apply(join(sort(b_new,b), sort(b_old,b)), z, iif(b_new.val_sum=b_old.val_sum, 1,0)), sum(z))" >> $OUTFILE

iquery -aq "op_count(c_new)" >> $OUTFILE
iquery -aq "op_count(c_old)" >> $OUTFILE
iquery -aq "aggregate(apply(join(sort(c_new,c), sort(c_old,c)), z, iif(c_new.val_avg=c_old.val_avg, 1,0)), sum(z))" >> $OUTFILE

iquery -aq "op_count(ac_new)" >> $OUTFILE
iquery -aq "op_count(ac_old)" >> $OUTFILE
iquery -aq "aggregate(apply(join(sort(ac_new,a,c), sort(ac_old,a,c)), z, iif(ac_new.val_avg=ac_old.val_avg or (ac_new.val_avg is null and ac_old.val_avg is null), 1,0)), sum(z))" >> $OUTFILE

iquery -aq "op_count(ab_new)" >> $OUTFILE
iquery -aq "op_count(ab_old)" >> $OUTFILE
iquery -aq "aggregate(
 apply(
  join(
   sort(ab_new,a,b),
   sort(ab_old,a,b)
  ),
  z,
  iif(
   (ab_new.val_max=ab_old.val_max or (ab_new.val_max is null and ab_old.val_max is null)) and
   (ab_new.val_var=ab_old.val_var or (ab_new.val_var is null and ab_old.val_var is null)),
   1,
   0)
  ),
  sum(z)
 )" >> $OUTFILE

iquery -aq "aggregate(grouped_aggregate(foo, count(*), count(b), i), sum(count), sum(b_count))" >> $OUTFILE

diff $OUTFILE $MYDIR/test.expected && echo "$(basename $0) succeeded"
