package etlflow.webserver.api

import cats.data.{Kleisli, OptionT}
import doobie.hikari.HikariTransactor
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlFlowHelper.{EtlFlowTask, UserArgs, UserAuth}
import etlflow.utils.CacheHelper
import etlflow.utils.db.Query.{getUser, logger}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Request}
import org.http4s.util.CaseInsensitiveString
import scalacache.Cache
import pdi.jwt.{Jwt, JwtAlgorithm}
import com.github.t3hnar.bcrypt._
import zio.Task
import zio.interop.catz._

object Authentication extends Http4sDsl[EtlFlowTask] with ApplicationLogger {
  def middleware(service: HttpRoutes[EtlFlowTask], authEnabled: Boolean, cache: Cache[String]): HttpRoutes[EtlFlowTask] = Kleisli {
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

  def login(args: UserArgs,transactor: HikariTransactor[Task],cache: Cache[String]): Task[UserAuth] =  {
    getUser(args.user_name,transactor).fold(ex => {
      logger.error("Error in fetching user from db => " + ex.getMessage)
      UserAuth("Invalid User/Password", "")
    }, user => {
      if (args.password.isBcryptedBounded(user.password)) {
        val user_data = s"""{"user":"${user.user_name}", "role":"${user.user_role}"}""".stripMargin
        val token = Jwt.encode(user_data, "secretKey", JwtAlgorithm.HS256)
        logger.info(s"New token generated for user ${user.user_name}")
        CacheHelper.putKey(cache, token, token, Some(CacheHelper.default_ttl))
        UserAuth("Valid User", token)
      } else {
        UserAuth("Invalid User/Password", "")
      }
    })
  }
}
