# Transformation and Actions
- At runtime we do not build RDDs during TRANSFORMATIONS. 
- All we are building is an EXECUTION PLAN.
- Only when we run an ACTION (make calculations happen and get results) does anything happens

# DAG and SparkUI
- localhost:4040
- ![img.png](public/img.png)
- ![DAG.png](public/DAG.png)
- In SparkUI we have list of Jobs (actions performed in our script roughly)
- sortByKey is an transformation but still generates a job in Job list

# Narrow vs Wide Transformations
- How does spark decide partitioning? -> It depends on the input source for the RDD. 
   Text file => chunks of data
   Some other sources => Hashes of each string line is calculated and hashes are distributed in different partitions
- Now transformations are sent to each Partition : Block of code running on Partition is called **TASK**