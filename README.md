QA & A

`1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?`
By altering the parallelism and partitioning configurations, we can increase or decrease the performance of Spark, specifically as seen in the 'processedRowsPerSecond' metric in the MicroBatchExecution progress reporting.

`2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?`
I had a very difficult time answering this, since the Spark setup on the Workspaces outputs such a huge volume of noise and it is nearly impossible to catch when a progress report appears. I think for further iterations of this course, this needs to be addressed in some manner.

That said, I would imagine that tweaking `spark.sql.shuffle.partitions` and `spark.default.parallelism` to ensure they match the hardware configuration in use would be highly beneficial.

I also found that changing the `maxRatePerPartition` option for the `readStream` function more than doubled my output in terms of `processedRowsPerSecond`.
