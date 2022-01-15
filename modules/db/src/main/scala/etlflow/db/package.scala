package etlflow

import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import zio.blocking.Blocking
import zio.{Has, ZLayer}

package object db {

  private[etlflow] type DBServerEnv = Has[server.DBServerApi.Service]
  type DBEnv                        = Has[DBApi.Service]

  def liveDB(db: JDBC, pool_name: String = "EtlFlow-Pool", pool_size: Int = 2): ZLayer[Blocking, Throwable, DBEnv] =
    CP.layer(db, pool_name, pool_size) >>> DB.live

  private[etlflow] def liveFullDB(
      db: JDBC,
      pool_name: String = "EtlFlow-Pool",
      pool_size: Int = 2
  ): ZLayer[Blocking, Throwable, DBEnv with DBServerEnv] =
    CP.layer(db, pool_name, pool_size) >>> (DB.live ++ server.DB.live)

  private[etlflow] def liveFullDBWithLog(
      db: JDBC,
      job_run_id: String,
      pool_name: String = "EtlFlow-Pool",
      pool_size: Int = 2
  ): ZLayer[Blocking, Throwable, DBEnv with LogEnv with DBServerEnv] =
    CP.layer(db, pool_name, pool_size) >>> (DB.live ++ log.DB.live(job_run_id) ++ server.DB.live)
}
