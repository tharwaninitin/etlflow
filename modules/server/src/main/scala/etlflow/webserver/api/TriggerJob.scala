package etlflow.webserver.api

import cats.effect.{ContextShift, Sync, Timer}
import doobie.hikari.HikariTransactor
import etlflow.utils.{Config, EtlFlowUtils, RequestValidator}
import etlflow.{EtlJobName, EtlJobProps}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import zio.{Semaphore, Task, _}

import scala.reflect.runtime.universe.TypeTag

class TriggerJob[F[_]: Sync: ContextShift: Timer] extends Http4sDsl[F] with etlflow.executor.Executor {


  object jobName extends QueryParamDecoderMatcher[String]("job_name")
  object props   extends OptionalQueryParamDecoderMatcher[String]("props")

  def triggerEtlJob[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](jobSemaphores: Map[String, Semaphore],transactor: HikariTransactor[Task],etl_job_name_package:String,config:Config,jobQueue: Queue[(String,String,String,String)]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "runjob" :? jobName(name) +& props(props)  => {
      val output = RequestValidator.validator(name,props)
      output match {
        case Right(output) => Runtime.default.unsafeRun(runEtlJobsFromApi[EJN, EJP](output, transactor, jobSemaphores(output.name), config, etl_job_name_package,jobQueue).map(x => Ok("Job Name: " + x.get.name + " ---> " + " Properties :" + x.get.props.map(x => x))))
        case Left(error)   => Ok(error)
      }
    }
  }
}
