package etlflow

import zio.Has

package object log {
  type LoggerEnv = Has[LoggerApi.Service]
  type SlackEnv = Has[SlackApi.Service]
}
