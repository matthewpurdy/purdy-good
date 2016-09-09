package purdygood.spark.accumulo

import org.apache.accumulo.core.client.{Connector, Instance}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

import purdygood.common.cli.CliManager
import purdygood.spark.accumulo.cli.{WriteCliManager, WriteCliArgs}
import purdygood.spark.accumulo.context.AccumuloContext
import purdygood.spark.accumulo.model.WriteRunArgs

object SparkAccumuloWriteMain extends App {

  override def main(args: Array[String]) {
    val filterList: List[String] = List("global", "write")
    val clientManager: CliManager = new WriteCliManager(args, filterList)

    System.out.println("timestamp          => " + WriteCliArgs.timestamp)
    System.out.println("jobType            => " + WriteCliArgs.jobType)
    System.out.println("accumuloInstance   => " + WriteCliArgs.accumuloInstanceName)
    System.out.println("accumuloZookeepers => " + WriteCliArgs.accumuloZookeepers)
    System.out.println("accumuloUsername   => " + WriteCliArgs.accumuloUsername)
    System.out.println("accumuloPassword   => " + WriteCliArgs.accumuloPassword)
    System.out.println("outputTableName    => " + WriteCliArgs.outputTableName)

    val writeRunArgs: WriteRunArgs = WriteRunArgs(WriteCliArgs.timestamp, WriteCliArgs.jobType,
                                                  new AccumuloContext(WriteCliArgs.accumuloInstanceName,
                                                                      WriteCliArgs.accumuloZookeepers,
                                                                      WriteCliArgs.accumuloUsername,
                                                                      WriteCliArgs.accumuloPassword),
                                                  WriteCliArgs.outputTableName)

    val timestamp = writeRunArgs.timestamp
    val jobName = "myspark-read-" + timestamp;
    val sc = new SparkContext(new SparkConf().setAppName(jobName))
    val conf = sc.hadoopConfiguration;
    val job = Job.getInstance(conf);

    val instanceName  = writeRunArgs.accumuloContext.instanceName
    val zookeepers = writeRunArgs.accumuloContext.zookeepers
    val principal = writeRunArgs.accumuloContext.username
    val principalPasswd = writeRunArgs.accumuloContext.password
    val token = writeRunArgs.accumuloContext.token
    val instance: Instance = writeRunArgs.accumuloContext.instance
    val connector: Connector = writeRunArgs.accumuloContext.connector
    val auths = new Authorizations("public")
    val tableName = writeRunArgs.outputTable
    //val timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date);
    //val jobName = "myspark-write-" + timestamp;
    //val sc = new SparkContext(new SparkConf().setAppName(jobName))
    
    //val instanceName  = "instance"
    //val zookeepers = "localhost:2181"
    //val principal = "root"
    //val principalPasswd = "toor"
    //val token = new PasswordToken(principalPasswd)
    //val instance: Instance = new ZooKeeperInstance(instanceName, zookeepers)
    //val connector: Connector = instance.getConnector(principal, token)
    //val auths = new Authorizations("public")
    //val tableName = "mytable2"
    //val conf = sc.hadoopConfiguration;
    //val job = Job.getInstance(conf);
    
    //Configure the job conf with our accumulo properties
    AccumuloOutputFormat.setConnectorInfo(job, principal, token)
    
    val clientConfig =  new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeepers)
    AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);
    
    AccumuloOutputFormat.setDefaultTableName(job, tableName)
    
    // Create an RDD using the job
    
    val key1 = new Key("row", "cf", "cq1", "public")
    val s1: String = "val1"
    val val1 = new Value(s1.getBytes)
    val key2 = new Key("row", "cf", "cq2", "public", new java.util.Date().getTime)
    val s2: String = "val2"
    val val2 = new Value(s2.getBytes)
    val key3 = new Key("row", "cf", "cq3", "public")
    val s3: String = "val3"
    val val3 = new Value(s3.getBytes)
    
    val colWrapper1 = new AccumuloColumnWrapper(Common.extractSafeKey(key1), Common.extractSafeValue(val1))
    val colWrapper2 = AccumuloColumnWrapper(Common.extractSafeKey(key2), Common.extractSafeValue(val2))
    val colWrapper3 = AccumuloColumnWrapper(Common.extractSafeKey(key3), Common.extractSafeValue(val3))
    
    val columns = Array(colWrapper1, colWrapper2, colWrapper3)
    
    val rdd = sc.parallelize(columns, 1)
    
    for (i <- 1 to 3) { println("") }
    println("###############################################################################") 
    println("###############################################################################") 
    println("###############################################################################") 
    println("#####***** rdd.class    => " + rdd.getClass)
    println("#####***** rdd.count    => " + rdd.count)
    println("#####***** rdd.first    => " + rdd.first)
    rdd.take(3).foreach(println)
    println("###############################################################################") 
    println("###############################################################################") 
    println("###############################################################################") 
    for (i <- 1 to 3) { println("") }
    
    val mutationRdd: RDD[(Text, Mutation)] = rdd.map({ column =>
      val mutation: Mutation = new Mutation(column.key.row)
      mutation.put(column.key.cf, column.key.cq, new ColumnVisibility(column.key.cv), column.key.ts, column.value.value)
      (new Text(tableName), mutation)
    })
    mutationRdd.saveAsNewAPIHadoopFile(instanceName,
                                       classOf[Void],
                                       classOf[Mutation],
                                       classOf[AccumuloOutputFormat],
                                       job.getConfiguration)
  }
}

