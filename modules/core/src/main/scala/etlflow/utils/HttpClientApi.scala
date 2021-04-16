package etlflow.utils

import org.slf4j.{Logger, LoggerFactory}
import sttp.client3.{asStringAlways, basicRequest}
import zio.Task
import sttp.client3.httpclient.zio._
import zio._
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import sttp.client3._
import sttp.model.MediaType

case class PostData(body: Seq[(String,String)])

object HttpClientApi {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def post(url: String, params: Either[String, Seq[(String, String)]],
           headers: Map[String, String],
           log_response: Boolean,
           connectionTimeOut: Int,
           readTimeOut: Int): Task[Response[String]] = {

    implicit val postBodySerializer: BodySerializer[PostData] = { p: PostData =>
      val serialized = s"${p.body}"
      StringBody(serialized, "UTF-8", MediaType.TextPlain)
    }

    val request =
      params match {
        case Left(value) =>
          basicRequest
            .body(value)
            .readTimeout(Duration(readTimeOut, MILLISECONDS))
            .headers(headers)
            .post(uri"${url}")
        case Right(value) =>
          basicRequest
            .body(postBodySerializer(PostData(value)))
            .readTimeout(Duration(readTimeOut, MILLISECONDS))
            .headers(headers)
            .post(uri"${url}")
      }

    val response = for {
      op <- HttpClientZioBackend.managed().use { backend =>
        request.send(backend)
      }
    } yield {

      logger.info(s"Request Headers: ${op.headers}")
      logger.info(s"Response Code: ${op.code}")
      logger.info(s"Response Headers: ${op.headers}")

      if (log_response) logger.info("Response Body: " + op.body)
      logger.info("#" * 100)

      op.body match {
        case Left(value) => {
          logger.error(s"Failed with Response code: ${op.code.code}")
          throw new RuntimeException(s"Failed with Response code: ${op.code}")
        }
        case Right(value) => Response.ok(value)
      }
    }
    response
  }

  def get(url: String, params: Seq[(String, String)] = Nil,
          headers: Map[String, String],
          log_response: Boolean,
          connectionTimeOut: Int,
          readTimeOut: Int): Task[Response[String]] = {

    val request = basicRequest
      .headers(headers)
      .readTimeout(Duration(readTimeOut, MILLISECONDS))
      .get(uri"$url?$params")
      .response(asStringAlways)

    val response = for {
      op <- HttpClientZioBackend.managed().use { backend =>
        request.send(backend)
      }
    } yield {

      logger.info(s"Request Headers: ${op.headers}")
      logger.info(s"Response Code: ${op.code}")
      logger.info(s"Response Headers: ${op.headers}")

      if (log_response) logger.info("Response Body: " + op.body)
      logger.info("#" * 100)

      if (op.code.code == 204 || op.code.code == 200 || op.code.code == 201) {
        op
      }
      else {
        logger.error(s"Failed with Response code: ${op.code}")
        throw new RuntimeException(s"Failed with Response code: ${op.code}")
      }
    }
    response
  }

  def put(url: String,
          data: String,
          headers: Map[String, String],
          log_response: Boolean,
          connectionTimeOut: Int,
          readTimeOut: Int
         ): Task[Response[String]] =  {

    val request = basicRequest
      .headers(headers)
      .body(data)
      .readTimeout(Duration(readTimeOut, MILLISECONDS))
      .put(uri"$url")
      .response(asStringAlways)

    val response = for {
      op <- HttpClientZioBackend.managed().use { backend =>
        request.send(backend)
      }
    } yield {
      logger.info(s"Request Headers: ${op.headers}")
      logger.info(s"Response Code: ${op.code}")
      logger.info(s"Response Headers: ${op.headers}")
      if (log_response) logger.info("Response Body: " + op.body)
      logger.info("#" * 100)

      if (op.code.code == 204 || op.code.code == 200 || op.code.code == 201) {
        op
      }
      else {
        logger.error(s"Failed with Response code: ${op.code}")
        throw new RuntimeException(s"Failed with Response code: ${op.code}")
      }
    }
    response
  }
}