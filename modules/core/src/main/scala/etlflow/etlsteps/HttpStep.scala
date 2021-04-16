package etlflow.etlsteps

import etlflow.utils.{HttpClientApi, JsonCirce, LoggingLevel}
import io.circe.Decoder
import zio._
import sttp.client3._
sealed trait HttpMethod

object HttpMethod {
  case object GET extends HttpMethod
  case object POST extends HttpMethod
  case object PUT extends HttpMethod
}

case class HttpStep(
                     name: String,
                     url: String,
                     http_method: HttpMethod,
                     params: Either[String, Seq[(String,String)]] = Left(""),
                     headers: Map[String,String] = Map.empty,
                     log_response: Boolean = false,
                     connectionTimeOut : Int = 10000,
                     readTimeOut : Int = 150000
                   )
  extends EtlStep[Unit, Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpStep: $name")
    etl_logger.info(s"URL: $url")
    etl_logger.info(s"ConnectionTimeOut: $connectionTimeOut")
    etl_logger.info(s"ReadTimeOut: $readTimeOut")

    http_method match {
      case HttpMethod.POST =>
        HttpClientApi.post(url, params, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit
      case HttpMethod.GET =>
        params match {
          case Left(_) => HttpClientApi.get(url, Nil, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit
          case Right(value) => HttpClientApi.get(url, value, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit
        }
      case HttpMethod.PUT =>
        params match {
          case Left(value) => HttpClientApi.put(url, value, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit
          case Right(_)    => HttpClientApi.put(url, "", headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit
        }
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "url" -> url,
      "http_method" -> http_method.toString
    )
}

case class HttpResponseStep(
                             name: String,
                             url: String,
                             http_method: HttpMethod,
                             params: Either[String, Seq[(String,String)]] = Left(""),
                             headers: Map[String,String] = Map.empty,
                             log_response: Boolean = false,
                             connectionTimeOut : Int = 10000,
                             readTimeOut : Int = 150000
                           )
  extends EtlStep[Unit, Response[String]] {

  final def process(in: =>Unit): Task[Response[String]] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpResponseStep: $name")
    etl_logger.info(s"URL: $url")

    http_method match {
      case HttpMethod.POST =>
        HttpClientApi.post(url, params, headers, log_response,connectionTimeOut,readTimeOut)
      case HttpMethod.GET =>
        params match {
          case Left(_) => HttpClientApi.get(url, Nil, headers, log_response,connectionTimeOut,readTimeOut)
          case Right(value) => HttpClientApi.get(url, value, headers, log_response,connectionTimeOut,readTimeOut)
        }
      case HttpMethod.PUT =>
        params match {
          case Left(value) => HttpClientApi.put(url, value, headers, log_response,connectionTimeOut,readTimeOut)
          case Right(_)    => HttpClientApi.put(url, "", headers, log_response,connectionTimeOut,readTimeOut)
        }
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "url" -> url,
      "http_method" -> http_method.toString
    )
}

case class HttpParsedResponseStep[T: Decoder](
                                               name: String,
                                               url: String,
                                               http_method: HttpMethod,
                                               params: Either[String, Seq[(String,String)]] = Left(""),
                                               headers: Map[String,String] = Map.empty,
                                               log_response: Boolean = false,
                                               connectionTimeOut : Int = 10000,
                                               readTimeOut : Int = 150000
                                             )
  extends EtlStep[Unit, T] {

  final def process(in: =>Unit): Task[T] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpParsedResponseStep: $name")
    etl_logger.info(s"URL: $url")

    http_method match {
      case HttpMethod.POST =>
        HttpClientApi.post(url, params, headers, log_response,connectionTimeOut,readTimeOut).map(x => JsonCirce.convertToObject[T](x.body))
      case HttpMethod.GET =>
        params match {
          case Left(_) => HttpClientApi.get(url, Nil, headers, log_response,connectionTimeOut,readTimeOut).map(x => JsonCirce.convertToObject[T](x.body))
          case Right(value) => HttpClientApi.get(url, value, headers, log_response,connectionTimeOut,readTimeOut).map(x => JsonCirce.convertToObject[T](x.body))
        }
      case HttpMethod.PUT =>
        params match {
          case Left(value) => HttpClientApi.put(url, value, headers, log_response,connectionTimeOut,readTimeOut).map(x => JsonCirce.convertToObject[T](x.body))
          case Right(_)    => HttpClientApi.put(url, "", headers, log_response,connectionTimeOut,readTimeOut).map(x => JsonCirce.convertToObject[T](x.body))
        }
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "url" -> url,
      "http_method" -> http_method.toString
    )
}
