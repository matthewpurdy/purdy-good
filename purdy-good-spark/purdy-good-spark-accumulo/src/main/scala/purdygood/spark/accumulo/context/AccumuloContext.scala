package purdygood.spark.accumulo.context

import org.apache.accumulo.core.client.admin.{SecurityOperations, TableOperations}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, Instance, ZooKeeperInstance}

class AccumuloContext(accumuloInstanceName: String, accumuloZookeepers: String,
                      accumuloUsername: String, accumuloPassword: String) {
  val instanceName: String = accumuloInstanceName
  val zookeepers: String = accumuloZookeepers
  val username: String = accumuloUsername
  val password: String = accumuloPassword
  val token = new PasswordToken(password)
  val instance: Instance = new ZooKeeperInstance(instanceName, zookeepers)
  val connector: Connector = instance.getConnector(username, token)
  val tableOps: TableOperations = connector.tableOperations()
  val securityOps: SecurityOperations = connector.securityOperations()
}
