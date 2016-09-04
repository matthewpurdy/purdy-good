#!/bin/bash

SPARK_HOME=/opt/spark
script_path=$(readlink -f $0)
parent_path=$(dirname $script_path)
target_path=$parent_path/../target
src_path=$parent_path/../src
test_resources_path=$src_path/test/resources
log4j_path=$test_resources_path/log4j.properties

cp $log4j_path /tmp/

#driver_classpath='.'
#for x in $(find $ACCUMULO_HOME/lib -name "*.jar"); do driver_classpath=$driver_classpath:$x; done

driver_classpath=${ACCUMULO_HOME}/lib/accumulo-core.jar:${ACCUMULO_HOME}/lib/accumulo-fate.jar:${ACCUMULO_HOME}/lib/accumulo-trace.jar:${ACCUMULO_HOME}/lib/guava.jar

echo ""
echo "SPARK_HOME  => $SPARK_HOME"
echo "script_path => $script_path"
echo "parent_path => $parent_path"
echo ""
echo ""
echo $SPARK_HOME/bin/spark-submit
echo     --verbose                     
echo     --master local[1]              
echo     --driver-memory 1G             
echo     --executor-cores 1             
echo     --driver-class-path ${driver_classpath}
echo     --driver-java-options "-Dlog4j.configuration=file:///tmp/log4j.properties"
echo     --class purdygood.spark.accumulo.SparkAccumuloReadRun $target_path/purdy-good-spark-accumulo-1.0.0.jar
echo "$@"

$SPARK_HOME/bin/spark-submit                                                     \
     --verbose                                                                   \
     --master local[1]                                                           \
     --driver-memory 1G                                                          \
     --driver-class-path ${driver_classpath}                                     \
     --executor-cores 1                                                          \
     --driver-java-options "-Dlog4j.configuration=file:///tmp/log4j.properties"  \
     --class purdygood.spark.accumulo.SparkAccumuloReadRun $target_path/purdy-good-spark-accumulo-1.0.0.jar $@

