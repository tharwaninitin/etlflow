package etlflow.etlsteps

import etlflow.utils.HttpClientApi
import zio.Task

class HttpStep(
                    val name: String,
                    val http_method:String,
                    val url: String,
                    val jsonBody: Option[String] = None,
                    val headers: Map[String,String] = Map("content-type"->"application/json")
                  )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpPostStep Query Step: $name")
    etl_logger.info(s"URL : $url")

    if(http_method=="post") {
      etl_logger.info(s"HTTP Method : " + http_method)
      Task(HttpClientApi.post(url, jsonBody,headers))
    }
    else {
      etl_logger.info(s"HTTP Method : " + http_method)
      Task(HttpClientApi.get(url,headers))
    }
  }

  override def getStepProperties(level: String): Map[String, String] = Map("Url" -> url,"Http Method" -> http_method)
}

object HttpStep {
  def apply(name: String,http_method:String,url: String,jsonBody: Option[String] = None,headers:Map[String,String] = Map("content-type"->"application/json")): HttpStep =
    new HttpStep(name,http_method,url,jsonBody,headers)
}


