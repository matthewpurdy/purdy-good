package purdygood.common.cli

import org.kohsuke.args4j.CmdLineParser
import scala.collection.JavaConversions._
import scala.io.Source

abstract class CliManager() {

  val timestampParameter: String = "--timestamp"
  val configFilepathParameter: String = "--config.filepath"
  val jobTypeParameter: String = "--job.type"
  val defaultFilterList: List[String] = List("global")
  var jobType: String = ""

  def this(args: Array[java.lang.String]) {
    this()
    init(args, defaultFilterList)
  }

  def this(args: Array[java.lang.String], configFilters: List[String]) {
    this()
    init(args, configFilters)
  }

  protected def init(args: Array[java.lang.String], configFilters: List[String]): Unit = {
    val cmdArgs = parseArgs(args, configFilters)
    for(a <- cmdArgs) {
      System.out.println("###### ClientManager.init: " + a)
    }
    val clientParser: CmdLineParser = createCmdLineParser(jobType)
    clientParser.parseArgument(cmdArgs.toList)
  }

  protected def createCmdLineParser(jobType: String): CmdLineParser {}

  protected def parseArgs(args: Array[java.lang.String], configFilters: List[String]): Array[java.lang.String] = {
    val ret = new java.util.ArrayList[String]()

    for(a <- args) {
      System.out.println("*** " + a)
    }

    var timestampDex = args.indexOf(timestampParameter)
    timestampDex += 1
    val timestamp = args(timestampDex)
    ret.append(timestampParameter)
    ret.append(timestamp)

    var jobTypeDex = args.indexOf(jobTypeParameter)
    jobTypeDex += 1
    jobType = args(jobTypeDex)
    ret.append(jobTypeParameter)
    ret.append(jobType)

    var configDex = args.indexOf(configFilepathParameter)
    configDex += 1
    val configFilepath = args(configDex)
    ret.append(configFilepathParameter)
    ret.append(configFilepath)


    for(line <- Source.fromFile(configFilepath).getLines) {
      if(line.trim.length != 0 && !line.trim.startsWith("#")) {
        configFilters.foreach({ filter =>
          if(line.trim.startsWith(filter)) {
            val pair = line.split("=")
            ret.append("--" + pair(0).substring(pair(0).indexOf(".") + 1))
            ret.append(pair(1))
          }
        })
      }
    }

    return ret.map(_.toString).toArray
  }
}
