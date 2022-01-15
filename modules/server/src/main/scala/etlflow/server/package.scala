package etlflow

import etlflow.core.CoreEnv
import etlflow.db.DBServerEnv
import etlflow.json.JsonEnv
import zio.{Has, RIO}

package object server {
  private[etlflow] type APIEnv        = Has[Service]
  private[etlflow] type ServerEnv     = APIEnv with CoreEnv with JsonEnv with DBServerEnv
  private[etlflow] type ServerTask[A] = RIO[ServerEnv, A]
}
