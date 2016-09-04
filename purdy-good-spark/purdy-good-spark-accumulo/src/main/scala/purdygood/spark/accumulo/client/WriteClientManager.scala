package purdygood.spark.accumulo.client

import org.kohsuke.args4j.CmdLineParser

class WriteClientManager(args: Array[java.lang.String], configFilters: List[String])
    extends ClientManager(args, configFilters) {

  protected def createCmdLineParser(jobType: String): CmdLineParser = {
    new CmdLineParser(WriteClientArgs)
  }

}
