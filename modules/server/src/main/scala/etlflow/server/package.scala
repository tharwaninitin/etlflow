package etlflow

import etlflow.db.DBServerEnv
import etlflow.json.JsonEnv
import zio.{Has, RIO, ZEnv}

package object server {
  private[etlflow] type APIEnv        = Has[Service]
  private[etlflow] type ServerEnv     = APIEnv with ZEnv with JsonEnv with DBServerEnv
  private[etlflow] type ServerTask[A] = RIO[ServerEnv, A]
}
