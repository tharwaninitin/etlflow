package etlflow.webserver.api

import cats.data.{Kleisli, OptionT}
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlFlowHelper.EtlFlowTask
import etlflow.utils.CacheHelper
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Request}
import org.http4s.util.CaseInsensitiveString
import scalacache.Cache
import zio.interop.catz._

object AuthMiddleware extends Http4sDsl[EtlFlowTask] with ApplicationLogger {
  def apply(service: HttpRoutes[EtlFlowTask], authEnabled: Boolean, cache: Cache[String]): HttpRoutes[EtlFlowTask] = Kleisli {
    req: Request[EtlFlowTask] =>
      if(authEnabled) {
        req.headers.get(CaseInsensitiveString("Authorization")) match {
          case Some(value) =>
            val token = value.value
            CacheHelper.getKey(cache,token) match {
              case Some(_) => service(req)
              case None =>
                logger.warn(s"Invalid/Expired token $token")
                OptionT.liftF(Forbidden())
            }
          case None =>
            logger.warn("Header not present. Invalid Request !!")
            OptionT.liftF(Forbidden())
        }
      } else{
        //Return response as it is when authentication is disabled
        service(req)
      }
  }
}
