package etlflow.task

import etlflow.http.{HttpApi, HttpMethod}
import sttp.client3.Response
import zio._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
case class HttpRequestTask(
    name: String,
    url: String,
    method: HttpMethod,
    params: Either[String, Map[String, String]] = Right(Map.empty),
    headers: Map[String, String] = Map.empty,
    log: Boolean = false,
    connectionTimeout: Int = 10000,
    readTimeout: Int = 150000,
    allowUnsafeSSL: Boolean = false
) extends EtlTask[Any, Response[String]] {

  override protected def process: Task[Response[String]] = {
    logger.info("#" * 50)
    logger.info(s"Starting HttpRequestTask: $name")
    logger.info(s"URL: $url")
    logger.info(s"ConnectionTimeOut: $connectionTimeout")
    logger.info(s"ReadTimeOut: $readTimeout")
    logger.info(s"AllowUnsafeSSL: $allowUnsafeSSL")
    HttpApi.execute(method, url, params, headers, log, connectionTimeout, readTimeout, allowUnsafeSSL)
  }

  override def getTaskProperties: Map[String, String] = Map("url" -> url, "http_method" -> method.toString)
}
