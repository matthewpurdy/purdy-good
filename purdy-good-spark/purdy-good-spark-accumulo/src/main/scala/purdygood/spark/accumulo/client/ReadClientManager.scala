package purdygood.spark.accumulo.client

import org.kohsuke.args4j.CmdLineParser

import scala.collection.JavaConversions._
import scala.io.Source

class ReadClientManager(args: Array[java.lang.String], configFilters: List[String])
    extends ClientManager(args, configFilters) {

  protected def createCmdLineParser(jobType: String): CmdLineParser = {
    new CmdLineParser(ReadClientArgs)
  }

}
