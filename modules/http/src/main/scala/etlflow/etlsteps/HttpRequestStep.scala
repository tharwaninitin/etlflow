package etlflow.etlsteps

import etlflow.http.{HttpApi, HttpMethod}
import sttp.client3.Response
import zio._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
case class HttpRequestStep(
    name: String,
    url: String,
    method: HttpMethod,
    params: Either[String, Map[String, String]] = Right(Map.empty),
    headers: Map[String, String] = Map.empty,
    log: Boolean = false,
    connection_timeout: Int = 10000,
    read_timeout: Int = 150000,
    allow_unsafe_ssl: Boolean = false
) extends EtlStep[Any, Response[String]] {

  protected def process: Task[Response[String]] = {
    logger.info("#" * 50)
    logger.info(s"Starting HttpRequestStep: $name")
    logger.info(s"URL: $url")
    logger.info(s"ConnectionTimeOut: $connection_timeout")
    logger.info(s"ReadTimeOut: $read_timeout")
    HttpApi.execute(method, url, params, headers, log, connection_timeout, read_timeout, allow_unsafe_ssl)
  }

  override def getStepProperties: Map[String, String] = Map(
    "url"         -> url,
    "http_method" -> method.toString
  )
}
