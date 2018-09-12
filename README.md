Intro and Assumptions:

This code is tested on yarn mode.
This code assumes that a path value for HDFS_CONSTANTS_FILE_LOC would be provided in the constants file in the common package(this file in HDFS should contain the mandatory properties). The code reads the messages from kafka and stores them on to HBase with a status column which specifies if the message violates the rules or not.
The code also does not really bother about the value in signal as it was a bit confusing for me to understand on signal with ALT suffix greater than 4.
The code is written in Scala 2.10.6 and Spark 1.6.1(we currently use these versions and it is difficult to set up a new cluster with later versions). More details can be found in the POM.xml.
This is a streaming application consuming data from kafka.


Discussion questions:

1. Conceptual approach and trade offs:
The code utilizes the functionalities exposed by spark in its streaming library. The iteration happens at the partition level and in each partition, the records are iterated one after the other.
In order to handle failures when the system/cluster goes down, we store offsets manually to HBase. Upon resuming, we start processing data at the same offset there by avoiding data loss. This can also pose as a trade off sometimes because we have to query HBase every time. But it is something that can be accommodated as we want to avoid data loss.
The data is stored onto HBase with a UUID as the row key. If we have an elaborated scenario, we can have a much better row key which would help us fetch data faster when querying.
For each cycle of spark, we have to connect to HBase twice in this scenario. Connecting to it once can make it much smoother.

2.
a. Runtime performance: This was tested with a kafka topic with only 3 partitions. Spark conf was 3 exec, 1g ram, 1 core. With this low configuration, it took less than 1 min to complete loading into HBase.
b. Computational complexity id O(n).
c. Bottlenecks: More resources would yield more performance with a lot of data. We have to reprocess the data from the previous partiton in case the system goes down.

3. Improvements:
Call HBase only once rather than twice like now.
Exception handling is not done very well but that part can be handled in a much better way.
