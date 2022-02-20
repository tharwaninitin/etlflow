package etlflow.webserver

import cache4s.{default_ttl, Cache}
import etlflow.server.model.{UserArgs, UserAuth}
import etlflow.db.DBServerEnv
import etlflow.server.DBServerApi
import etlflow.utils.ApplicationLogger
import org.mindrot.jbcrypt.BCrypt
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http._
import zio.{RIO, Task}

case class Authentication(cache: Cache[String, String], secretKey: Option[String]) extends ApplicationLogger {

  final val secret = secretKey.getOrElse("EtlFlowCrypt2020")

  private[etlflow] def validateJwt(token: String): Boolean = Jwt.isValid(token, secret, Seq(JwtAlgorithm.HS256))

  private[etlflow] def isCached(token: String): Option[String] = cache.get(token)

  private[etlflow] def middleware[R, E](app: HttpApp[R, E]): HttpApp[R, E] = Http.flatten {
    Http.fromFunction[Request] { req =>
      req.headerValue("X-Auth-Token") match {
        case Some(token) =>
          if (validateJwt(token)) {
            isCached(token) match {
              case Some(_) => app
              case None =>
                logger.warn(s"Expired token $token")
                Http.forbidden("Not allowed!")
            }
          } else {
            logger.warn(s"Invalid token $token")
            Http.forbidden("Request Not allowed due to Invalid token!")
          }
        case None =>
          logger.warn("Header not present. Invalid Request !!")
          Http.forbidden("Request Not allowed. Header not present!")
      }
    }
  }

  private[etlflow] def login(args: UserArgs): RIO[DBServerEnv, UserAuth] =
    DBServerApi
      .getUser(args.user_name)
      .foldM(
        ex => {
          logger.error("Error in fetching user from db => " + ex.getMessage)
          Task(UserAuth("Invalid User/Password", ""))
        },
        user =>
          if (BCrypt.checkpw(args.password, user.password)) Task {
            val user_data = s"""{"user":"${user.user_name}", "role":"${user.user_role}"}""".stripMargin
            val token     = Jwt.encode(user_data, secret, JwtAlgorithm.HS256)
            logger.info(s"New token generated for user ${user.user_name}")
            cache.put(token, token, Some(default_ttl))
            UserAuth("Valid User", token)
          }
          else {
            Task(UserAuth("Invalid User/Password", ""))
          }
      )
}
