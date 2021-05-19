package etlflow.webserver

import etlflow.DBEnv
import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.api.{APIEnv, Service}
import etlflow.jdbc.DBServerEnv
import etlflow.utils.{JsonJackson, RequestValidator}
import zhttp.http.HttpApp
import zhttp.http._
import zhttp.http.Response
import zhttp.http.Method._
import zio.blocking.Blocking
import zio.clock.Clock
object ZioRestAPI {

  def oldRestApi: HttpApp[APIEnv with DBServerEnv with DBEnv with Blocking with Clock, Throwable] =
    HttpApp.collectM {
    case req@GET -> Root / "api" /"runjob" =>
      val job_name =  req.url.queryParams.getOrElse("job_name",List("Job"))
      val props = req.url.queryParams.getOrElse("props",List.empty)
      RequestValidator(job_name(0),if(props.isEmpty) None  else Some(props.mkString(","))) match {
        case Right(output) =>
          Service.runJob(output,"Rest API")
            .map(x =>  Response.jsonString(s"""{"message" -> "Job ${x.name} submitted successfully"}"""))
      }
  }

  def newRestApi: HttpApp[APIEnv with DBServerEnv with DBEnv with Blocking with Clock, Throwable] =
    HttpApp.collectM {
    case req@POST  -> Root /  "restapi" / "runjob" / name =>
      val props = io.circe.parser.decode[Map[String, String]](req.getBodyAsString.getOrElse("")) match {
        case Left(_) => None
        case Right(value) => Some(value.map(kv => Props(kv._1,kv._2)).toList)
      }
      val etlJobArgs = EtlJobArgs(name,props)
      Service.runJob(etlJobArgs,"New Rest API")
        .map(x =>  Response.jsonString(s"""${JsonJackson.convertToJsonByRemovingKeys(x,List.empty)}"""))
  }
}
