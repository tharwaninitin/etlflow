package etlflow

import etlflow.log.LogEnv
import zio.blocking.Blocking
import zio.clock.Clock

package object core {
  type CoreEnv = Blocking with Clock
  type CoreLogEnv = CoreEnv with LogEnv
}
