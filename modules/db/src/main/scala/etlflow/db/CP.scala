package etlflow.db

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}
import zio._

object CP extends ApplicationLogger {

  def create(db: JDBC, poolName: String = "EtlFlowPool", poolSize: Int = 2): ZIO[Scope, Throwable, String] =
    ZIO.acquireRelease(ZIO.attempt {
      logger.info(s"Creating connection pool $poolName with driver ${db.driver} with pool size $poolSize")
      val _ = Class.forName(db.driver)
      ConnectionPool.add(poolName, db.url, db.user, db.password, ConnectionPoolSettings(maxSize = poolSize))
      poolName
    })(_ =>
      ZIO.attempt {
        logger.info(s"Closing connection pool $poolName")
        ConnectionPool.close(poolName)
      }.orDie
    )
  def layer(db: JDBC, poolName: String, poolSize: Int): TaskLayer[String] = ZLayer.scoped(create(db, poolName, poolSize))
}
