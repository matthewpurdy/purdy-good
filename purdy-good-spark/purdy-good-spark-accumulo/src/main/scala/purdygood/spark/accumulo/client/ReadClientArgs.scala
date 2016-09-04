package purdygood.spark.accumulo.client

import org.kohsuke.args4j.Option

object ReadClientArgs {

  @Option(name = "--timestamp", required = true, usage = "14 numeric timestamp; e.g. 20010203040506")
  var timestamp: String = null

  @Option(name = "--config.filepath", required = true, usage = "config filepath defining arguments")
  var configFilepath: String = null

  @Option(name = "--job.type", required = true, usage = "job type (read, write, analytics, etc)")
  var jobType: String = null

  @Option(name = "--accumulo.instance.name", required = true, usage = "accumulo instance name")
  var accumuloInstanceName: String = null

  @Option(name = "--accumulo.zookeepers", required = true, usage = "accumulo zookeepers")
  var accumuloZookeepers: String = null

  @Option(name = "--accumulo.username", required = true, usage = "accumulo username")
  var accumuloUsername: String = null

  @Option(name = "--accumulo.password", required = true, usage = "accumulo password")
  var accumuloPassword: String = null

  @Option(name = "--input.table.name", required = true, usage = "accumulo input table name")
  var inputTableName: String = null

}
