# Column Store Database System with functionality of range partitioning

Code based on skeleton from Harvard CS165.

This should be filled with a workflow.

Start by giving (you may have to run automake twice):

> `automake --add-missing`

> `autoreconf`

> `./configure`

Then there you can compile:

> `make`

---
To create a dataset and workload for a test:

> `cd testtools`

and edit `gen.cfg` file to customize your test

> `./dataGen.py`

> `./dataGen_dbinfo.py` 

> `./workGen.py`

then move the b_* folder and the workload file into the `data` folder for tests

The workload generated is in ascii format, for partition decision function to work, 
you need to genereate parsed past workload through the db server, function doSomething
in file src/utils/utils.c does this job.

---
To partition the dataset currently in database according to a specific workload

feed the client binary:

> `partition(<col_var>, "workload_num")`


To run the partition decision algorithm only, feed the client binary:

> `partition_decision(<col_var>, "workload_num")`


To execute many workload commands in one command, feed the client binary:

> `exec_work("relativeworkloadfilePATH")`


To print the results for partitioned table, feed client binary:

> `show_tbl(<col_var>)`


More db operations can be found under folder src/database/dsl.c

