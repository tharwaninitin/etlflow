package etlflow

import etlflow.log.LogEnv
import etlflow.model.Credential.JDBC
import zio.blocking.Blocking
import zio.{Has, ZLayer}

package object db {

  type DBEnv = Has[DBApi.Service]

  def liveDB(db: JDBC, poolName: String = "EtlFlow-Pool", poolSize: Int = 2): ZLayer[Blocking, Throwable, DBEnv] =
    CP.layer(db, poolName, poolSize) >>> DB.live

  def liveDBWithLog(
      db: JDBC,
      jobRunId: String,
      poolName: String = "EtlFlow-Pool",
      poolSize: Int = 2
  ): ZLayer[Blocking, Throwable, DBEnv with LogEnv] =
    CP.layer(db, poolName, poolSize) >>> (DB.live ++ log.DB.live(jobRunId))
}
