#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#  @author Karthik Kulkarni
#
#

shopt -s expand_aliases

source parameters.sh

# Setting Color codes
green='\e[0;32m'
red='\e[0;31m'
NC='\e[0m' # No Color

sep='==================================='

hssize="10000000"
prefix="100GB"
HDFS_DIR="benchmark"

SPARK_CONF="spark.driver.memory=${SPARK_DRIVER_MEMORY} spark.executor.memory=${SPARK_EXECUTOR_MEMORY}"

usage()
{
cat << EOF
TPCx-HS style benchmark for Spark.
usage: $0 options

This script run the terasort for Spark with TPCx-HS benchmark style.
TPCx-HS is trademark of Transaction Processing Performance Council.

OPTIONS:
   -h  Help
   -g  <option 1,2,3 or 4>
       0   Run benchmark for 1GB
       1   Run benchmark for 100GB
       2   Run benchmark for 300GB
       3   Run benchmark for 1TB
       4   Run benchmark for 3TB
       5   Run benchmark for 10TB
       6   Run benchmark for 30TB
       7   Run benchmark for 100TB
       8   Run benchmark for 300TB
       9   Run benchmark for 1PB

   Example: ./run.sh -g 2

EOF
}

getopts "hg:" OPTION
     case ${OPTION} in
         h) usage
             exit 1
             ;;
         g)  sze=$OPTARG
 			 case ${sze} in
 			    0) hssize="1000000000"
 			       prefix="1GB"
 			       ;;
				1) hssize="100000000000"
				   prefix="100GB"
				     ;;
				2) hssize="300000000000"
				   prefix="300GB"
					 ;;
				3) hssize="1000000000000"
				   prefix="1TB"
					 ;;
				4) hssize="3000000000000"
				   prefix="3TB"
					 ;;
				5) hssize="10000000000000"
				   prefix="10TB"
					 ;;
				6) hssize="30000000000000"
				   prefix="30TB"
					 ;;
				7) hssize="100000000000000"
				   prefix="100TB"
					 ;;
				8) hssize="300000000000000"
				   prefix="300TB"
					 ;;
				9) hssize="1000000000000000"
				   prefix="1PB"
					 ;;
				?) hssize="100000000000"
				   prefix="100GB"
				   ;;
			 esac
             ;;
         ?)  echo -e "${red}Choose the correct options${NC}"
             usage
             exit
             ;;
     esac

if [ -f ./result-"$prefix".log ]; then
   mv ./result-"$prefix".log ./result-"$prefix".log.`date +%Y%m%d%H%M%S`
fi

echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Running $prefix test${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}HSsize is $hssize${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}All Output will be logged to file ./result-$prefix.log${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

## BIGDATA BENCHMARK SUITE ##

# Note for 1TB (1000000000000), input for teragen => 10000000000 (so many 100byte words)
# Similarly for 1GB, input for teragen => 10000000  (so many 100byte words)
# Similarly for 1MB, input for teragen => 10000  (so many 100byte words)

hdfs dfs -ls
if [ $? != 0 ] ;then
  sudo -u ${HDFS_USER} hdfs dfs -mkdir /user/"$HADOOP_USER"
  sudo -u ${HDFS_USER} hdfs dfs -chown "$HADOOP_USER":"$HADOOP_USER" /user/"$HADOOP_USER"
fi

hdfs dfs -ls ${HDFS_DIR}
if [ $? != 0 ] ;then
  hdfs dfs -mkdir ${HDFS_DIR}
fi

for i in `seq 1 2`;
do

benchmark_result=1

echo -e "${green}$sep${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}Deleting Previous Data - Start - `date`${NC}" | tee -a ./result-"$prefix".log
hadoop fs -rm -r -skipTrash /user/"$HADOOP_USER"/${HDFS_DIR}/*
hadoop fs -expunge
sleep ${SLEEP_BETWEEN_RUNS}
echo -e "${green}Deleting Previous Data - End - `date`${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}$sep${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

start=`date +%s`
echo -e "${green}$sep${NC}" | tee -a ./result-"$prefix".log
echo -e "${green} Running TPCx-HS style benchmark for Spark - Run $i - Epoch $start ${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}$sep${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Starting TeraGen Run $i (output being return to ./logs/HSgen-time-run$i.txt)${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

# DataGen
mkdir -p ./logs
(time spark-submit --class ${TERAGEN_CLASS_NAME} --deploy-mode ${SPARK_DEPLOY_MODE} --master ${SPARK_MASTER_URL} --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" ${SPARK_TERASORT_JAR} ${hssize} /user/"${HADOOP_USER}"/"${HDFS_DIR}"/HSsort-input ) 2>&1 | (tee ./logs/HSgen-time-run${i}.txt)
result=$?

cat ./logs/HSgen-time-run${i}.txt >> ./result-"$prefix".log

if [ ${result} -ne 0 ]
then
echo -e "${red}======== TeraGen Result FAILURE ========${NC}" | tee -a ./result-"$prefix".log
benchmark_result=0
else
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}======== TeraGen Result SUCCESS ========${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}======== Time take by TeraGen = `grep real ./logs/HSgen-time-run${i}.txt | awk '{print $2}'`====${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
fi

echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Listing TeraGen output ${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
hdfs dfs -ls /user/"$HADOOP_USER"/"${HDFS_DIR}"/HSsort-input/* | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Starting TeraSort Run $i (output being return to ./logs/HSsort-time-run$i.txt)${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

(time spark-submit --class ${TERASORT_CLASS_NAME} --deploy-mode ${SPARK_DEPLOY_MODE} --master ${SPARK_MASTER_URL} --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" ${SPARK_TERASORT_JAR} /user/"${HADOOP_USER}"/"${HDFS_DIR}"/HSsort-input /user/"${HADOOP_USER}"/"${HDFS_DIR}"/HSsort-output) 2>&1 | (tee ./logs/HSsort-time-run${i}.txt)
result=$?

cat ./logs/HSsort-time-run${i}.txt >> ./result-"$prefix".log

if [ ${result} -ne 0 ]
then
echo -e "${red}======== TeraSort Result FAILURE ========${NC}" | tee -a ./result-"$prefix".log
benchmark_result=0
else
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}======== TeraSort Result SUCCESS =============${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}======== Time take by TeraSort = `grep real ./logs/HSsort-time-run${i}.txt | awk '{print $2}'`====${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
fi


echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Listing TeraSort output ${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
hdfs dfs -ls /user/"$HADOOP_USER"/"${HDFS_DIR}"/HSsort-output/* | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Starting TeraValidate ${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log

(time spark-submit --class ${TERAVAL_CLASS_NAME} --deploy-mode ${SPARK_DEPLOY_MODE} --master ${SPARK_MASTER_URL} --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" ${SPARK_TERASORT_JAR} /user/"${HADOOP_USER}"/"${HDFS_DIR}"/HSsort-output) 2>&1 | (tee ./logs/HSvalidate-time-run${i}.txt)
result=$?

cat ./logs/HSvalidate-time-run${i}.txt >> ./result-"$prefix".log

if [ ${result} -ne 0 ]
then
echo -e "${red}======== TeraValidate Result FAILURE ========${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
benchmark_result=0
else
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}======== TeraValidate Result SUCCESS =============${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}======== Time take by TeraValidate = `grep real ./logs/HSvalidate-time-run${i}.txt | awk '{print $2}'`====${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
fi

end=`date +%s`

if (($benchmark_result == 1))
then
total_time=`expr ${end} - ${start}`
total_time_in_hour=$(echo "scale=4;${total_time}/3600" | bc)
scale_factor=$(echo "scale=4;${hssize}/10000000000" | bc)
perf_metric=$(echo "scale=4;${scale_factor}/${total_time_in_hour}" | bc)

echo -e "${green}$sep============${NC}" | tee -a ./result-"${prefix}".log
echo -e "${green}TPCx-HS Performance Metric (HSph@SF) Report ${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}Test Run $i details: Total Time = $total_time ${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}                     Total Size = $hssize ${NC}" | tee -a ./result-"$prefix".log
echo -e "${green}                     Scale-Factor = $scale_factor ${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}TPCx-HS Performance Metric (HSph@SF): $perf_metric ${NC}" | tee -a ./result-"$prefix".log
echo "" | tee -a ./result-"$prefix".log
echo -e "${green}$sep============${NC}" | tee -a ./result-"$prefix".log

else
echo -e "${red}$sep${NC}" | tee -a ./result-"$prefix".log
echo -e "${red}No Performance Metric (HSph@SF) as some tests Failed ${NC}" | tee -a ./result-"$prefix".log
echo -e "${red}$sep${NC}" | tee -a ./result-"$prefix".log

fi


done
