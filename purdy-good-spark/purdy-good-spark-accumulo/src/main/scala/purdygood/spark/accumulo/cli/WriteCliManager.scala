package purdygood.spark.accumulo.cli

import purdygood.common.cli.CliManager
import org.kohsuke.args4j.CmdLineParser

class WriteCliManager(args: Array[java.lang.String], configFilters: List[String])
    extends CliManager(args, configFilters) {

  protected def createCmdLineParser(jobType: String): CmdLineParser = {
    new CmdLineParser(WriteCliArgs)
  }

}
