#HDFC and Apache Spark Steps
###Spark shell for python
```sh
module load python3
python -m pip install {user pyspark
pyspark
```
Download a textle to count the number of lines:```sh wget http://www.gutenberg.org/cache/epub/1661/pg1661.txt  ```
Run: ```sh spark-submit path/to/myapp.py path/to/pg1661.txt ```


You can ignore that output for now. Regardless, near the bottom of the output
you will see the output from the application:

```sh
2019-02-26 08:18:11 WARN NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... 2019-02-26 08:18:12 INFO SparkContext:54 - Running Spark version 2.4.0
2019-02-26 08:18:12 INFO SparkContext:54 - Submitted application: myapp.py
2019-02-26 08:18:12 INFO SecurityManager:54 - Changing view acls to: gguzun
2019-02-26 08:18:12 INFO SecurityManager:54 - Changing modify acls to: gguzun
2019-02-26 08:18:12 INFO SecurityManager:54 - Changing view acls groups to:
2019-02-26 08:18:12 INFO SecurityManager:54 - Changing modify acls groups to:
2019-02-26 08:18:12 INFO SecurityManager:54 - SecurityManager: authentication disabled; ui acls disabled; 2019-02-26 08:18:12 INFO Utils:54 - Successfully started service 'sparkDriver' on port 32819.
2019-02-26 08:18:12 INFO SparkEnv:54 - Registering MapOutputTracker
2019-02-26 08:18:12 INFO SparkEnv:54 - Registering BlockManagerMaster
2019-02-26 08:18:12 INFO BlockManagerMasterEndpoint:54 - Using org.apache.spark.storage.DefaultTopologyMapper 2019-02-26 08:18:12 INFO BlockManagerMasterEndpoint:54 - BlockManagerMasterEndpoint up
2019-02-26 08:18:12 INFO DiskBlockManager:54 - Created local directory at /tmp/blockmgr-3a256796-3f84-4c2a-8f48-7df5d7d39c74
2019-02-26 08:18:12 INFO MemoryStore:54 - MemoryStore started with capacity 366.3 MB
2019-02-26 08:18:12 INFO SparkEnv:54 - Registering OutputCommitCoordinator
2019-02-26 08:18:12 INFO log:192 - Logging initialized @2481ms
2019-02-26 08:18:12 INFO Server:351 - jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
2019-02-26 08:18:12 INFO Server:419 - Started @2552ms
2019-02-26 08:18:12 INFO AbstractConnector:278 - Started ServerConnector@1e10f6bf{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
2019-02-26 08:18:12 INFO Utils:54 - Successfully started service 'SparkUI' on port 4040.
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@5f99b832{/jobs,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@3e9d5d08{/jobs/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@6282bffa{/jobs/job,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@10f4ad04{/jobs/job/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@44d5d70f{/stages,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@33dfdd00{/stages/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@40ce4693{/stages/stage,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@521a7a3e{/stages/stage/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@60d25430{/stages/pool,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2f02806e{/stages/pool/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@34e1966a{/storage,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@35abd142{/storage/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@472a6013{/storage/rdd,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@7c109968{/storage/rdd/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@447a0246{/environment,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2fb0ea4a{/environment/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@3b8e2d57{/executors,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@43ad3897{/executors/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@57516a96{/executors/threadDump,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@7f9b288c{/executors/threadDump/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@35b546bd{/static,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@7b0a3991{/,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@235c373c{/api,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@68a3d4a7{/jobs/job/kill,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@32d59dcf{/stages/stage/kill,null,AVAILABLE,@Spark}
2019-02-26 08:18:12 INFO SparkUI:54 - Bound SparkUI to 0.0.0.0, and started at http://login:4040
2019-02-26 08:18:12 INFO Executor:54 - Starting executor ID driver on host localhost
2019-02-26 08:18:12 INFO Utils:54 - Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' 2019-02-26 08:18:12 INFO NettyBlockTransferService:54 - Server created on login:35386
2019-02-26 08:18:12 INFO BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy 2019-02-26 08:18:12 INFO BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, login, 2019-02-26 08:18:12 INFO BlockManagerMasterEndpoint:54 - Registering block manager login:35386 with 366.3 2019-02-26 08:18:12 INFO BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, login, 2019-02-26 08:18:12 INFO BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, login, 35386, 2019-02-26 08:18:13 INFO ContextHandler:781 - Started o.s.j.s.ServletContextHandler@17589b37{/metrics/json,null,AVAILABLE,@Spark}
2019-02-26 08:18:13 INFO MemoryStore:54 - Block broadcast_0 stored as values in memory (estimated size 2019-02-26 08:18:13 INFO MemoryStore:54 - Block broadcast_0_piece0 stored as bytes in memory (estimated 2019-02-26 08:18:13 INFO BlockManagerInfo:54 - Added broadcast_0_piece0 in memory on login:35386 (size: 2019-02-26 08:18:13 INFO SparkContext:54 - Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
2019-02-26 08:18:13 INFO FileInputFormat:249 - Total input paths to process : 1
2019-02-26 08:18:13 INFO SparkContext:54 - Starting job: count at /home/gguzun/myapp.py:5
2019-02-26 08:18:13 INFO DAGScheduler:54 - Got job 0 (count at /home/gguzun/myapp.py:5) with 2 output partitions
2019-02-26 08:18:13 INFO DAGScheduler:54 - Final stage: ResultStage 0 (count at /home/gguzun/myapp.py:5)
2019-02-26 08:18:13 INFO DAGScheduler:54 - Parents of final stage: List()
2019-02-26 08:18:13 INFO DAGScheduler:54 - Missing parents: List()
2019-02-26 08:18:13 INFO DAGScheduler:54 - Submitting ResultStage 0 (PythonRDD[2] at count at /home/gguzun/myapp.py:5), 2019-02-26 08:18:13 INFO MemoryStore:54 - Block broadcast_1 stored as values in memory (estimated size 2019-02-26 08:18:13 INFO MemoryStore:54 - Block broadcast_1_piece0 stored as bytes in memory (estimated 2019-02-26 08:18:13 INFO BlockManagerInfo:54 - Added broadcast_1_piece0 in memory on login:35386 (size: 2019-02-26 08:18:13 INFO SparkContext:54 - Created broadcast 1 from broadcast at DAGScheduler.scala:1161
2019-02-26 08:18:13 INFO DAGScheduler:54 - Submitting 2 missing tasks from ResultStage 0 (PythonRDD[2]
2019-02-26 08:18:13 INFO TaskSchedulerImpl:54 - Adding task set 0.0 with 2 tasks
2019-02-26 08:18:13 INFO TaskSetManager:54 - Starting task 0.0 in stage 0.0 (TID 0, localhost, executor 2019-02-26 08:18:13 INFO TaskSetManager:54 - Starting task 1.0 in stage 0.0 (TID 1, localhost, executor 2019-02-26 08:18:13 INFO Executor:54 - Running task 0.0 in stage 0.0 (TID 0)
2019-02-26 08:18:13 INFO Executor:54 - Running task 1.0 in stage 0.0 (TID 1)
2019-02-26 08:18:14 INFO HadoopRDD:54 - Input split: file:/home/gguzun/iorbench.log:0+3230
2019-02-26 08:18:14 INFO HadoopRDD:54 - Input split: file:/home/gguzun/iorbench.log:3230+3230
2019-02-26 08:18:14 INFO PythonRunner:54 - Times: total = 648, boot = 596, init = 52, finish = 0
2019-02-26 08:18:14 INFO PythonRunner:54 - Times: total = 649, boot = 591, init = 58, finish = 0
2019-02-26 08:18:14 INFO Executor:54 - Finished task 1.0 in stage 0.0 (TID 1). 1504 bytes result sent to 2019-02-26 08:18:14 INFO Executor:54 - Finished task 0.0 in stage 0.0 (TID 0). 1504 bytes result sent to 2019-02-26 08:18:14 INFO TaskSetManager:54 - Finished task 0.0 in stage 0.0 (TID 0) in 808 ms on localhost 2019-02-26 08:18:14 INFO TaskSetManager:54 - Finished task 1.0 in stage 0.0 (TID 1) in 793 ms on localhost 2019-02-26 08:18:14 INFO TaskSchedulerImpl:54 - Removed TaskSet 0.0, whose tasks have all completed, from 2019-02-26 08:18:14 INFO PythonAccumulatorV2:54 - Connected to AccumulatorServer at host: 127.0.0.1 port: 2019-02-26 08:18:14 INFO DAGScheduler:54 - ResultStage 0 (count at /home/gguzun/myapp.py:5) finished in 2019-02-26 08:18:14 INFO DAGScheduler:54 - Job 0 finished: count at /home/gguzun/myapp.py:5, took 0.963566 116 lines
2019-02-26 08:18:14 INFO SparkContext:54 - Invoking stop() from shutdown hook
2019-02-26 08:18:14 INFO AbstractConnector:318 - Stopped Spark@1e10f6bf{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
2019-02-26 08:18:14 INFO SparkUI:54 - Stopped Spark web UI at http://login:4040
2019-02-26 08:18:14 INFO MapOutputTrackerMasterEndpoint:54 - MapOutputTrackerMasterEndpoint stopped!
2019-02-26 08:18:14 INFO MemoryStore:54 - MemoryStore cleared
2019-02-26 08:18:14 INFO BlockManager:54 - BlockManager stopped
2019-02-26 08:18:14 INFO BlockManagerMaster:54 - BlockManagerMaster stopped
2019-02-26 08:18:14 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint:54 - OutputCommitCoordinator 2019-02-26 08:18:14 INFO SparkContext:54 - Successfully stopped SparkContext
2019-02-26 08:18:14 INFO ShutdownHookManager:54 - Shutdown hook called
2019-02-26 08:18:14 INFO ShutdownHookManager:54 - Deleting directory /tmp/spark-4b8f3cff-f7d4-414c-804b-2be239840f59
2019-02-26 08:18:14 INFO ShutdownHookManager:54 - Deleting directory /tmp/spark-8cfe90c2-016b-4d5e-b615-4e71712deeaf
2019-02-26 08:18:14 INFO ShutdownHookManager:54 - Deleting directory /tmp/spark-4b8f3cff-f7d4-414c-804b-2be239840f59/pyspark-431b1f8e-c059-4b5e-9216-f033bade8688
```


To run the application with 2 threads, launch it as
```sh
spark-submit --master 'local[2]' path/to/myapp.py path/to/pg1661.txt.
```
You can replace the \2" with any number. To use as many threads as are available on your system, launch the
application as
```sh
spark-submit --master 'local[*]' path/to/myapp.py path/to/pg1661.txt
```
###Word Count Example in python
spark-submit path/to/wc.py path/to/pg1661.txt path/to/output
