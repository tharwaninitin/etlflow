package etlflow.scheduler.api

import cats.data.{Kleisli, OptionT}
import etlflow.scheduler.api.EtlFlowHelper.EtlFlowTask
import etlflow.scheduler.util.CacheHelper
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Request}
import org.http4s.util.CaseInsensitiveString
import org.slf4j.{Logger, LoggerFactory}
import scalacache.Cache
import zio.interop.catz._

object AuthMiddleware extends Http4sDsl[EtlFlowTask] {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def apply(
             service: HttpRoutes[EtlFlowTask],
             authEnabled: Boolean,
             cache: Cache[String]
           ): HttpRoutes[EtlFlowTask] = Kleisli {
    req: Request[EtlFlowTask] =>
      if(authEnabled) {
        req.headers.get(CaseInsensitiveString("Authorization")) match {
          case Some(value) =>
            val token = value.value
            if (CacheHelper.getKey(cache,token).getOrElse("NA") == token) {
              //Return response as it it in case of success
              service(req)
            } else {
              //Return forbidden error as request is invalid
              logger.warn(s"Invalid token $token")
              OptionT.liftF(Forbidden())
            }
          case None =>
            logger.warn("Header not present. Invalid Request !! ")
            OptionT.liftF(Forbidden())
        }
      } else{
        //Return response as it is when authentication is disabled
        service(req)
      }
  }
}
