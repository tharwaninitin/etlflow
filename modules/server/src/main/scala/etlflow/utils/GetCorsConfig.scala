package etlflow.utils

import org.http4s.server.middleware.CORSConfig
import scala.concurrent.duration._

object GetCorsConfig {
  def apply(config: Option[WebServer]): CORSConfig = {
    val origins = config.map(_.allowedOrigins.getOrElse(Set.empty)).getOrElse(Set.empty)
    if (origins.isEmpty)
      CORSConfig(anyOrigin = false, allowCredentials = false, maxAge = 1.day.toSeconds)
    else
      CORSConfig(anyOrigin = false, allowedOrigins = origins, allowCredentials = false, maxAge = 1.day.toSeconds)
  }
}