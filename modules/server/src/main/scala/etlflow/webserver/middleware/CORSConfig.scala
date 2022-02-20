package etlflow.webserver.middleware

import etlflow.model.WebServer
import zhttp.http.middleware.Cors._

private[etlflow] object CORSConfig {
  def apply(config: Option[WebServer]): CorsConfig = {
    val origins = config.map(_.allowedOrigins.getOrElse(Set.empty)).getOrElse(Set.empty)
    if (origins.isEmpty)
      CorsConfig(anyOrigin = false, allowCredentials = false)
    else
      CorsConfig(anyOrigin = false, allowedOrigins = origins, allowCredentials = false)
  }
}
