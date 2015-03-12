HADOOP_USER=root
HDFS_USER=hdfs
SLEEP_BETWEEN_RUNS=60

## Spark Parametars
# Driver Memory
SPARK_DRIVER_MEMORY=512m

# Executor Memory
SPARK_EXECUTOR_MEMORY=1g

# DEPLOY_MODE one of 'cluster' or 'client'
SPARK_DEPLOY_MODE="client"

# Master URL for the cluster. 'spark://localhost:7077', 'yarn-client' or 'yarn-cluster'
SPARK_MASTER_URL="spark://localhost:7077"

# spark-terasort jar name
SPARK_TERASORT_JAR="spark-terasort-0.1.jar"

# Class Name
TERAGEN_CLASS_NAME="com.github.ehiggs.spark.terasort.TeraGen"
TERASORT_CLASS_NAME="com.github.ehiggs.spark.terasort.TeraSort"
TERAVAL_CLASS_NAME="com.github.ehiggs.spark.terasort.TeraValidate"
