package purdygood.spark.accumulo.cli

import purdygood.common.cli.CliManager
import org.kohsuke.args4j.CmdLineParser

import scala.collection.JavaConversions._
import scala.io.Source

class ReadCliManager(args: Array[java.lang.String], configFilters: List[String])
    extends CliManager(args, configFilters) {

  protected def createCmdLineParser(jobType: String): CmdLineParser = {
    new CmdLineParser(ReadCliArgs)
  }

}
