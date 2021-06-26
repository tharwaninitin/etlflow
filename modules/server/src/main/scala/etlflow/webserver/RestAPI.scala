package etlflow.webserver

import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.api.{ServerEnv, Service}
import etlflow.db.EtlJob
import etlflow.json.{Implementation, JsonApi}
import etlflow.utils.RequestValidator
import io.circe.generic.semiauto.deriveEncoder
import zhttp.http.Method._
import zhttp.http.{HttpApp, Response, _}

object RestAPI {

  implicit val caseDecoder = deriveEncoder[EtlJob]

  def oldRestApi: HttpApp[ServerEnv, Throwable] =
    HttpApp.collectM {
    case req@GET -> Root / "api" /"runjob" =>
      val job_name = req.url.queryParams.getOrElse("job_name",List("Job"))
      val props = req.url.queryParams.getOrElse("props",List.empty)
      RequestValidator(job_name.head, if(props.isEmpty) None else Some(props.mkString(","))).flatMap{output =>
        Service.runJob(output,"Rest API")
            .map(x =>  Response.jsonString(s"""{"message" -> "Job ${x.name} submitted successfully"}"""))
      }
  }

  def newRestApi: HttpApp[ServerEnv, Throwable] =
    HttpApp.collectM {
    case req@POST  -> Root /  "restapi" / "runjob" / name =>
      val props = io.circe.parser.decode[Map[String, String]](req.getBodyAsString.getOrElse("")) match {
        case Left(_) => None
        case Right(value) => Some(value.map(kv => Props(kv._1,kv._2)).toList)
      }
      for {
          etlJob   <- Service.runJob(EtlJobArgs(name,props),"New Rest API")
          json     <- JsonApi.convertToJsonByRemovingKeys(etlJob,List.empty)
          response = Response.jsonString(json.toString())
        } yield response

    }
}
