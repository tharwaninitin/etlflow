package etlflow

import etlflow.crypto.CryptoEnv
import etlflow.db.DBEnv
import etlflow.json.JsonEnv
import etlflow.log.{ConsoleEnv, LoggerEnv, SlackEnv}
import zio.blocking.Blocking
import zio.clock.Clock

package object core {
  type CoreEnv = DBEnv with JsonEnv with CryptoEnv with LoggerEnv with Blocking with Clock
  type JobEnv = CoreEnv with SlackEnv with ConsoleEnv
}
