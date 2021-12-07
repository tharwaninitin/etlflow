package etlflow

import zio.Has

package object log {
  type LogWrapperEnv = Has[LogWrapperApi.Service]
  type SlackLogEnv = Has[SlackApi.Service]
  type DBLogEnv = Has[DBApi.Service]
  type ConsoleLogEnv = Has[ConsoleApi.Service]
}
