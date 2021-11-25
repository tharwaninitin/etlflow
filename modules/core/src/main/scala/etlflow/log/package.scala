package etlflow

import zio.Has

package object log {
  type LoggerEnv = Has[LoggerApi.Service]
  type SlackEnv = Has[SlackApi.Service]
  type DBLogEnv = Has[DBApi.Service]
  type ConsoleEnv = Has[ConsoleApi.Service]
}
