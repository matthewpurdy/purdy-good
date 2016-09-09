package purdygood.spark.accumulo

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import purdygood.common.cli.CliManager
import purdygood.spark.accumulo.cli.{ReadCliManager, ReadCliArgs}
import purdygood.spark.accumulo.context.AccumuloContext
import purdygood.spark.accumulo.model.ReadRunArgs

object SparkAccumuloReadMain extends App {

  override def main(args: Array[String]) {
    val filterList: List[String] = List("global", "read")
    val clientManager: CliManager = new ReadCliManager(args, filterList)

    System.out.println("timestamp          => " + ReadCliArgs.timestamp)
    System.out.println("jobType            => " + ReadCliArgs.jobType)
    System.out.println("accumuloInstance   => " + ReadCliArgs.accumuloInstanceName)
    System.out.println("accumuloZookeepers => " + ReadCliArgs.accumuloZookeepers)
    System.out.println("accumuloUsername   => " + ReadCliArgs.accumuloUsername)
    System.out.println("accumuloPassword   => " + ReadCliArgs.accumuloPassword)
    System.out.println("inputTableName     => " + ReadCliArgs.inputTableName)

    val readRunArgs: ReadRunArgs = ReadRunArgs(ReadCliArgs.timestamp, ReadCliArgs.jobType,
                                               new AccumuloContext(ReadCliArgs.accumuloInstanceName,
                                                                   ReadCliArgs.accumuloZookeepers,
                                                                   ReadCliArgs.accumuloUsername,
                                                                   ReadCliArgs.accumuloPassword),
                                               ReadCliArgs.inputTableName)

    val timestamp = readRunArgs.timestamp
    val jobName = "myspark-read-" + timestamp;
    val sc = new SparkContext(new SparkConf().setAppName(jobName))
    val conf = sc.hadoopConfiguration;
    val job = Job.getInstance(conf);

    val instanceName  = readRunArgs.accumuloContext.instanceName
    val zookeepers = readRunArgs.accumuloContext.zookeepers
    val principal = readRunArgs.accumuloContext.username
    val principalPasswd = readRunArgs.accumuloContext.password
    val token = readRunArgs.accumuloContext.token
    val instance: Instance = readRunArgs.accumuloContext.instance
    val connector: Connector = readRunArgs.accumuloContext.connector
    val auths = new Authorizations("public")
    val tableName = readRunArgs.inputTable
    
    //Configure the job conf with our accumulo properties
    AbstractInputFormat.setConnectorInfo(job, principal, token)
    AbstractInputFormat.setScanAuthorizations(job, auths)
    
    val clientConfig = new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeepers)
    AbstractInputFormat.setZooKeeperInstance(job, clientConfig)

    println("SparkAccumuloReadRun.main: tableName => " + tableName)
    InputFormatBase.setInputTableName(job, tableName)
    // Create an RDD using the job
    val rdd = sc.newAPIHadoopRDD(job.getConfiguration(), 
                                  classOf[org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat], 
                                  classOf[org.apache.accumulo.core.data.Key], 
                                  classOf[org.apache.accumulo.core.data.Value]).map { case (key, value) => new AccumuloColumnWrapper(Common.extractSafeKey(key), Common.extractSafeValue(value)) }
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
  }
}

