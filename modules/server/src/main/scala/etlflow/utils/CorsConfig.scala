package etlflow.utils


import etlflow.schema.WebServer
import zhttp.http.CORSConfig

private [etlflow] object CorsConfig {
  def apply(config: Option[WebServer]): CORSConfig = {
    val origins = config.map(_.allowedOrigins.getOrElse(Set.empty)).getOrElse(Set.empty)
    if (origins.isEmpty)
      CORSConfig(anyOrigin = false, allowCredentials = false)
    else
      CORSConfig(anyOrigin = false, allowedOrigins = origins, allowCredentials = false)
  }
}