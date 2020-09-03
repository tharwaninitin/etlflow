package etlflow.etlsteps

import etlflow.utils.{HttpClientApi, JsonCirce, JsonJackson, LoggingLevel}
import io.circe.Decoder
import scalaj.http._
import zio.Task

sealed trait HttpMethod

object HttpMethod {
  case object GET extends HttpMethod
  case object POST extends HttpMethod
}

case class HttpStep(
                     name: String,
                     url: String,
                     http_method: HttpMethod,
                     params: Either[String, Seq[(String,String)]] = Left(""),
                     headers: Map[String,String] = Map.empty,
                     log_response: Boolean = false
                   )
  extends EtlStep[Unit, Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpStep: $name")
    etl_logger.info(s"URL: $url")

    http_method match {
      case HttpMethod.POST =>
        HttpClientApi.postUnit(url, params, headers, log_response)
      case HttpMethod.GET =>
        params match {
          case Left(_) => HttpClientApi.getUnit(url, Nil, headers, log_response)
          case Right(value) => HttpClientApi.getUnit(url, value, headers, log_response)
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
                             log_response: Boolean = false
                           )
  extends EtlStep[Unit, HttpResponse[String]] {

  final def process(in: =>Unit): Task[HttpResponse[String]] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpResponseStep: $name")
    etl_logger.info(s"URL: $url")

    http_method match {
      case HttpMethod.POST =>
        HttpClientApi.post(url, params, headers, log_response)
      case HttpMethod.GET =>
        params match {
          case Left(_) => HttpClientApi.get(url, Nil, headers, log_response)
          case Right(value) => HttpClientApi.get(url, value, headers, log_response)
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
                                                log_response: Boolean = false
                                              )
  extends EtlStep[Unit, T] {

  final def process(in: =>Unit): Task[T] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpParsedResponseStep: $name")
    etl_logger.info(s"URL: $url")

    http_method match {
      case HttpMethod.POST =>
        HttpClientApi.post(url, params, headers, log_response).map(x => JsonCirce.convertToObject[T](x.body))
      case HttpMethod.GET =>
        params match {
          case Left(_) => HttpClientApi.get(url, Nil, headers, log_response).map(x => JsonCirce.convertToObject[T](x.body))
          case Right(value) => HttpClientApi.get(url, value, headers, log_response).map(x => JsonCirce.convertToObject[T](x.body))
        }
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "url" -> url,
      "http_method" -> http_method.toString
    )
}
