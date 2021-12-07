package etlflow

import etlflow.log.{ConsoleLogEnv, DBLogEnv, LogWrapperEnv, SlackLogEnv}
import zio.blocking.Blocking
import zio.clock.Clock

package object core {
  type CoreEnv = Blocking with Clock
  type LogEnv = LogWrapperEnv with DBLogEnv with SlackLogEnv with ConsoleLogEnv
  type CoreLogEnv = CoreEnv with LogEnv
}
