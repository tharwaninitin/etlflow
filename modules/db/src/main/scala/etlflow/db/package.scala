package etlflow

import etlflow.audit.Audit
import etlflow.model.Credential.JDBC
import zio.TaskLayer

package object db {

  type DBEnv = DBApi.Service

  def liveDB(db: JDBC, poolName: String = "EtlFlow-Pool", poolSize: Int = 2): TaskLayer[DBEnv] =
    CP.layer(db, poolName, poolSize) >>> DB.live

  def liveDBWithLog(
      db: JDBC,
      jobRunId: String,
      poolName: String = "EtlFlow-Pool",
      poolSize: Int = 2
  ): TaskLayer[DBEnv with Audit] =
    CP.layer(db, poolName, poolSize) >>> (DB.live ++ audit.DB.live(jobRunId))
}
