package etlflow.utils

import org.slf4j.{Logger, LoggerFactory}
import scalaj.http._
import zio.{Task, ZIO}

object HttpClientApi {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def postUnit(url: String, params: Either[String,Seq[(String,String)]],
               headers:Map[String,String],
               log_response: Boolean,
               connectionTimeOut:Int,
               readTimeOut:Int): Task[Unit] =
    post(url, params, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit

  def post(url: String, params: Either[String,Seq[(String,String)]],
           headers:Map[String,String],
           log_response: Boolean,
           connectionTimeOut:Int,
           readTimeOut:Int): Task[HttpResponse[String]] = Task {
    val request: HttpRequest =
      params match {
        case Left(value) =>
          Http(url)
            .timeout(connTimeoutMs = connectionTimeOut, readTimeoutMs = readTimeOut)
            .postData(value)
            .headers(headers)
            .option(HttpOptions.allowUnsafeSSL)
        case Right(value) =>
          Http(url)
            .timeout(connTimeoutMs = connectionTimeOut, readTimeoutMs = readTimeOut)
            .postForm(value)
            .headers(headers)
            .option(HttpOptions.allowUnsafeSSL)
      }

    logger.info(s"Request Method: ${request.method}")
    logger.info(s"Request Headers: ${request.headers}")

    val response = request.asString

    logger.info(s"Response Code: ${response.code}")
    logger.info(s"Response Headers: ${response.headers}")
    if (log_response) logger.info("Response Body: " + response.body)
    logger.info("#"*100)

    if(response.code == 204 || response.code == 200 || response.code == 201) {
      response
    }
    else {
      logger.error(s"Failed with Response code: ${response.code}")
      throw new RuntimeException(s"Failed with Response code: ${response.code}")
    }
  }

  def getUnit(url: String, params: Seq[(String,String)] = Nil,
              headers:Map[String,String],
              log_response: Boolean,
              connectionTimeOut:Int,
              readTimeOut:Int): Task[Unit] =
    get(url, params, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit

  def get(url: String, params: Seq[(String,String)] = Nil,
          headers:Map[String,String],
          log_response: Boolean,
          connectionTimeOut:Int,
          readTimeOut:Int): Task[HttpResponse[String]] = Task {
    val request = Http(url)
      .timeout(connTimeoutMs = connectionTimeOut, readTimeoutMs = readTimeOut)
      .headers(headers)
      .params(params)
      .option(HttpOptions.allowUnsafeSSL)

    logger.info(s"Request Method: ${request.method}")
    logger.info(s"Request Headers: ${request.headers}")

    val response = request.asString

    logger.info(s"Response Code: ${response.code}")
    logger.info(s"Response Headers: ${response.headers}")
    if (log_response) logger.info("Response Body: " + response.body)
    logger.info("#"*100)

    if(response.code == 204 || response.code == 200 || response.code == 201) {
      response
    }
    else {
      logger.error(s"Failed with Response code: ${response.code}")
      throw new RuntimeException(s"Failed with Response code: ${response.code}")
    }
  }

  def putUnit(url: String,
              data : String,
              headers:Map[String,String],
              log_response: Boolean,
              connectionTimeOut:Int,
              readTimeOut:Int
             ): Task[Unit] =
    put(url, data, headers, log_response,connectionTimeOut,readTimeOut) *> ZIO.unit

  def put(url : String,
          data : String,
          headers : Map[String,String],
          log_response : Boolean,
          connectionTimeOut:Int,
          readTimeOut:Int
         ) : Task[HttpResponse[String]] = Task {
    val request = Http(url)
      .timeout(connTimeoutMs = connectionTimeOut, readTimeoutMs = readTimeOut)
      .put(data)
      .headers(headers)
      .option(HttpOptions.allowUnsafeSSL)

    logger.info(s"Request Method: ${request.method}")
    logger.info(s"Request Headers: ${request.headers}")

    val response = request.asString

    logger.info(s"Response Code: ${response.code}")
    logger.info(s"Response Headers: ${response.headers}")
    if (log_response) logger.info("Response Body: " + response.body)
    logger.info("#"*100)

    if(response.code == 204 || response.code == 200 || response.code == 201) {
      response
    }
    else {
      logger.error(s"Failed with Response code: ${response.code}")
      throw new RuntimeException(s"Failed with Response code: ${response.code}")
    }
  }
}