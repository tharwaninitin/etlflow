package etlflow.webserver.api

import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.EtlFlowHelper.EtlFlowTask
import etlflow.utils.{Config, RequestValidator}
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import zio.{Semaphore, _}
import scala.reflect.runtime.universe.TypeTag
import zio.interop.catz._
import io.circe._
import org.http4s.circe._

object RestAPI extends Http4sDsl[EtlFlowTask] with etlflow.executor.Executor  {

  object jobName extends QueryParamDecoderMatcher[String]("job_name")
  object props   extends OptionalQueryParamDecoderMatcher[String]("props")

  def routes[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](jobSemaphores: Map[String, Semaphore], etl_job_name_package:String,config:Config,jobQueue: Queue[(String,String,String,String)]): HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case GET -> Root / "runjob" :? jobName(name) +& props(props) =>
      RequestValidator(name,props) match {

        case Right(output) => runActiveEtlJob[EJN](output, jobSemaphores(output.name), config, etl_job_name_package,"Rest-API",jobQueue)
                              .flatMap(x => Ok(Json.obj("message" -> Json.fromString(s"Job ${x.name} submitted successfully"))))
                              .absorb // This will help propagating defects further(if defects occurs inside effect => runActiveEtlJob)
                                      // https://github.com/zio/zio/issues/1082
        case Left(error)   => BadRequest(error)
      }
  }
}
