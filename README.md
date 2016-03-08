# grouped_aggregate

A prototype operator for aggregation grouped by attributes, dimensions or combinations thereof. 

## Example
```
$ iquery -anq "store(apply(build(<a:double>[x=1:10,5,0,y=1:10,5,0], random()%10), b, string(random()%2)),test)" 
Query was executed successfully

$ iquery -aq "grouped_aggregate(test, sum(a) as total, b)"
{instance_id,value_no} b,total
{0,0} '1',290
{2,0} '0',231

$ iquery -aq "grouped_aggregate(test, avg(a), count(*), b, x, 'output_chunk_size=10000')"
{instance_id,value_no} b,x,a_avg,count
{0,0} '0',5,6.33333,3
{0,1} '1',6,4.5,6
{0,2} '0',7,3,5
{0,3} '1',2,4.5,4
{0,4} '1',10,5.66667,3
{0,5} '1',8,5.33333,6
{1,0} '0',1,4.75,4
{1,1} '0',6,5.5,4
{1,2} '1',5,5.57143,7
{1,3} '1',7,4.8,5
{1,4} '1',3,5.16667,6
{1,5} '1',9,4.75,8
{1,6} '0',4,4,4
{2,0} '0',9,5,2
{2,1} '1',4,5.33333,6
{3,0} '0',8,6.25,4
{3,1} '0',10,5.85714,7
{3,2} '0',2,5.83333,6
{3,3} '0',3,7.25,4
{3,4} '1',1,5.33333,6
```

## More formally
```
grouped_aggregate(input_array, aggregate_1(input_1) [as alias_1], group_1, 
                  [, aggregate_2(input_2),...]
                  [, group_2,...]
                  [, 'setting=value'])
Where
  input_array            :: any SciDB array
  aggregate_1...N        :: any SciDB-registered aggregate
  input_1..N             :: any attribute in input
  group_1..M             :: any attribute or dimension in input
The operator must be invoked with at least one aggregate and at least one group.

Optional tuning settings:
  input_sorted=<true/false>     :: a hint that the input array is sorted by groups, or that, generally, 
                                   aggregate group values are likely repeated often. Defaults to true 
                                   if aggregating by non-last dimension, false otherwise.
  max_table_size=MB             :: the amount of memory (in MB) that the operator's hash table structure
                                   may consume. Once the table exceeds this size, new aggregate groups 
                                   are placed into a spillover array defaults to the merge-sort-buffer 
                                   configuration setting.
  num_hash_buckets=N            :: the number of hash buckets to allocate in the hash table. Larger 
                                   values improve speed but also use more memory. Should be a prime. 
                                   Default: 1,000,037.
  spill_chunk_size=C            :: the chunk size of the spill-over array. Defaults to 100,000. Should
                                   be smaller if there are are many of group-by attributes or aggregates. 
                                   TBD: automate
  merge_chunk_size=C            :: the chunk size of the array used to transfer data between instances. 
                                   Defaults to 100,000. Should be smaller if there are many group-by 
                                   attributes or instances. TBD: automate.
  output_chunk_size=C           :: the chunk size of the final output array. Defaults to 100,000. 
                                   TBD: automate.
  
Returned array contains one attribute for each group, and one attribute for each aggregated value. 
The dimensions are superfluous.

When grouping by attributes, an attribute value of null (or any missing code) constitutes an invalid 
group that is not included in the output. All inputs associated with invalid groups are ignored. 
When grouping by multiple attributes, a null or missing value in any one of the attributes makes the
entire group invalid.
```

## Installation

Easiest with https://github.com/paradigm4/dev_tools:
```
iquery -aq "install_github('paradigm4/grouped_aggregate')"
iquery -aq "load_library('grouped_aggregate')"
```
