package purdygood.spark.accumulo.model

import purdygood.spark.accumulo.context.AccumuloContext

case class ReadRunArgs(timestamp: String, jobType: String, accumuloContext: AccumuloContext, inputTable: String)
case class WriteRunArgs(timestamp: String, jobType: String, accumuloContext: AccumuloContext, outputTable: String)

