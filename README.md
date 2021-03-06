# SparkSQL
Performing several queries on a flat-text file converted to SQL Table (DataFrame in Spark) and on several JSON files converted to SQL Tables as well (DataFrames).

Firstly, we are given a flat-text file that is made of different types of wines and their attributes in terms of properties as numeric values. From that, we transform this file into a Spark DataFrame, prodiving a schema to it about each attribute, so that we are able to perform SQL Queries like a traditional SQL Table, performing 2 queries on such a DataFrame.

Secondly, we extend even beyond the capabilities of Spark DataFrames by dealing with JSON files. In this regard, we use several JSON files of laureates and nobel prizes to get different results by means of SparkSQL and the concept of DataFrame.

To summarize, all together is aimed at showcasing the potential uses of SparkSQL and the concept of DataFrame in Spark, which is intrinsic to it, in order to be applied to a variety of different contexts where we have large data files in a distributed fashion over different data nodes (with HDFS underneath, for instance) and we would like to treat them as a unique SQL Table to retrieve the same information in the same manner we would retrieve in a local RDBMS, with the addition of the power of parallelization, mainly in Main Memory, that Sparks provides.
