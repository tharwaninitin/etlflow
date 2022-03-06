package etlflow

import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import zio.blocking.Blocking
import zio.{Has, ZLayer}

package object db {

  type DBEnv = Has[DBApi.Service]

  def liveDB(db: JDBC, pool_name: String = "EtlFlow-Pool", pool_size: Int = 2): ZLayer[Blocking, Throwable, DBEnv] =
    CP.layer(db, pool_name, pool_size) >>> DB.live

  def liveDBWithLog(
      db: JDBC,
      job_run_id: String,
      pool_name: String = "EtlFlow-Pool",
      pool_size: Int = 2
  ): ZLayer[Blocking, Throwable, DBEnv with LogEnv] =
    CP.layer(db, pool_name, pool_size) >>> (DB.live ++ log.DB.live(job_run_id))
}
