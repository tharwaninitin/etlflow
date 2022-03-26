package etlflow.db

import etlflow.model.Credential.JDBC
import etlflow.utils.ApplicationLogger
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}
import zio._

object CP extends ApplicationLogger {
  def create(db: JDBC, poolName: String = "EtlFlowPool", poolSize: Int = 2): Managed[Throwable, String] =
    Managed.make(Task {
      logger.info(s"Creating connection pool $poolName with driver ${db.driver} with pool size $poolSize")
      val _ = Class.forName(db.driver)
      ConnectionPool.add(poolName, db.url, db.user, db.password, ConnectionPoolSettings(maxSize = poolSize))
      poolName
    })(_ =>
      Task {
        logger.info(s"Closing connection pool $poolName")
        ConnectionPool.close(poolName)
      }.orDie
    )
  def layer(db: JDBC, poolName: String, poolSize: Int): Layer[Throwable, Has[String]] =
    create(db, poolName, poolSize).toLayer
}
