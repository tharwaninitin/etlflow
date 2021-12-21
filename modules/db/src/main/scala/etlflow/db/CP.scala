package etlflow.db

import etlflow.schema.Credential.JDBC
import etlflow.utils.ApplicationLogger
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}
import zio._

object CP extends ApplicationLogger {
  def create(db: JDBC, pool_name: String = "EtlFlowPool", pool_size: Int = 2): Managed[Throwable, String] =
    Managed.make(Task{
      logger.info(s"Creating connection pool $pool_name with driver ${db.driver} with pool size $pool_size")
      Class.forName(db.driver)
      ConnectionPool.add(pool_name, db.url, db.user, db.password, ConnectionPoolSettings(maxSize = pool_size))
      pool_name
    })(_ => Task{
      logger.info(s"Closing connection pool $pool_name")
      ConnectionPool.close(pool_name)
    }.orDie)
  def layer(db: JDBC, pool_name: String, pool_size: Int): Layer[Throwable, Has[String]] =
    create(db, pool_name, pool_size).toLayer
}
