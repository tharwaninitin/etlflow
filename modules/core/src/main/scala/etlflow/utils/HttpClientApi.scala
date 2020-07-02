package etlflow.utils
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http._
object HttpClientApi {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def post(url: String, request_body: Option[String],headers:Map[String,String]): HttpResponse[String] = {
      logger.info("-" * 50)
      println(request_body)

      val request = Http(url)
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
        .postData(request_body.get)
        .headers(headers)

      logger.info("-" * 50)
      val response = request.asString
      logger.info("-" * 50)
      logger.info("Status Code: " + response.code + "\tResponse Body: " + response.body + "\tResponse Headers: " + response.headers)
      response
  }

  def get(url: String,headers:Map[String,String]): HttpResponse[String] = {
    logger.info("-" * 50)
    val request = Http(url)
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
      .headers(headers)
      .option(HttpOptions.allowUnsafeSSL)
    logger.info("-" * 50)
    val response = request.asString
    logger.info("-" * 50)
    logger.info("Status Code: " + response.code + "\tResponse Body: " + response.body + "\tResponse Headers: " + response.headers)

    if(response.code == 204 ||  response.code == 200)
      logger.info(" Callback Response code : " + response.code)
    else
      logger.info(" Failed Callback Response code : " + response.code)

    response
  }
}