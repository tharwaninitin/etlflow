package etlflow

import etlflow.json.JsonEnv
import etlflow.log.{ConsoleEnv, DBLogEnv, LoggerEnv, SlackEnv}
import zio.blocking.Blocking
import zio.clock.Clock

package object core {
  type CoreEnv = DBLogEnv with LoggerEnv with JsonEnv with Blocking with Clock
  type JobEnv = CoreEnv with SlackEnv with ConsoleEnv
}
