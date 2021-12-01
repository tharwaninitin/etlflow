package etlflow

import etlflow.log.{ConsoleEnv, DBLogEnv, LoggerEnv, SlackEnv}
import zio.blocking.Blocking
import zio.clock.Clock

package object core {
  type CoreEnv = Blocking with Clock
  type StepEnv = CoreEnv with LoggerEnv with DBLogEnv with SlackEnv with ConsoleEnv
}
