package purdygood.spark.accumulo

import org.apache.accumulo.core.data.{Key,Value}

case class KeyWrapper(row: String, cf: String, cq: String, cv: String, ts: Long)
case class ValueWrapper(value: String)
case class AccumuloColumnWrapper(key: KeyWrapper, value: ValueWrapper)

object Common {
  
  def extractSafeKey(key: Key): KeyWrapper = {
    new KeyWrapper(key.getRow().toString(), key.getColumnFamily().toString(), key.getColumnQualifier().toString(), key.getColumnVisibility().toString, key.getTimestamp())
  }
  
  def extractSafeValue(value: Value): ValueWrapper = {
    new ValueWrapper(value.toString())
  }
}
