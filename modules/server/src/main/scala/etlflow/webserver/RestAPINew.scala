package etlflow.webserver

import etlflow.api.Schema._
import etlflow.api.{ServerTask, Service}
import io.circe.generic.auto._
import org.http4s.HttpRoutes
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.tapir.swagger.http4s.SwaggerHttp4s
import zio.interop.catz._

object RestAPINew {

  case class RestEtlJobArgs(name: String, props: Option[Map[String,String]])

  private val runJobEndpointInput: EndpointInput[RestEtlJobArgs] =
    path[String]("job_name").description("Job Name").and(jsonBody[Option[Map[String,String]]]).mapTo(RestEtlJobArgs)

  private val runJobEndpointDescription: Endpoint[RestEtlJobArgs, String, EtlJob, Any] =
    endpoint.post
      .in("runjob")
      .in(runJobEndpointInput)
      .errorOut(stringBody)
      .out(jsonBody[EtlJob])

  private def runJob(args: RestEtlJobArgs): ServerTask[Either[String,EtlJob]] = {
    val params = EtlJobArgs(args.name,Some(args.props.getOrElse(Map.empty).map(kv => Props(kv._1,kv._2)).toList))
    Service.runJob(params,"New Rest API").mapError(e => e.getMessage).either
  }

  private val runJobRoute: HttpRoutes[ServerTask] = Http4sServerInterpreter.toRoutes(runJobEndpointDescription)(runJob)

  private val yaml: String = {
    import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
    import sttp.tapir.openapi.circe.yaml._
    OpenAPIDocsInterpreter.toOpenAPI(List(runJobEndpointDescription), "EtlFlow API", "1.0").toYaml
  }
  val swaggerRoute: HttpRoutes[ServerTask] = new SwaggerHttp4s(yaml).routes[ServerTask]

  val routes: HttpRoutes[ServerTask] = runJobRoute
}
