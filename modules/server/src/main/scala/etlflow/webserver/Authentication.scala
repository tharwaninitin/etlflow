package etlflow.webserver

import etlflow.api.Schema.{UserArgs, UserAuth}
import etlflow.cache.{Cache, CacheApi, CacheEnv, default_ttl}
import etlflow.db.{DBServerApi, DBServerEnv}
import etlflow.utils.ApplicationLogger
import org.mindrot.jbcrypt.BCrypt
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http._
import zio.Runtime.default.unsafeRun
import zio.{RIO, Task, ZEnv, ZIO}

case class Authentication(cache: Cache[String], secretkey: Option[String]) extends  ApplicationLogger {

  final val secret = secretkey.getOrElse(etlflow.utils.Defaults.secretkey)
  private [etlflow] def validateJwt(token: String): Boolean = Jwt.isValid(token, secret, Seq(JwtAlgorithm.HS256))

  private [etlflow] def isCached(token: String): ZIO[ZEnv, Throwable, Option[String]] =
    CacheApi.get(cache, token).provideCustomLayer(etlflow.cache.Implementation.live)

  private [etlflow] def middleware[R, E](app: HttpApp[R, E]): HttpApp[R, E] =  Http.flatten {
    Http.fromFunction[Request](req => {
      val headerValue = if(req.getHeader("Authorization").isDefined)
        req.getHeader("Authorization")
      else
        req.getHeader("X-Auth-Token")
      headerValue match {
        case Some(value) =>
          val token = value._2.toString
          if(validateJwt(token)) {
            unsafeRun(isCached(token)) match {
              case Some(_) => app
              case None =>
                logger.warn(s"Expired token $token")
                Http.forbidden("Not allowed!")
            }
          }
          else {
            logger.warn(s"Invalid token $token")
            Http.forbidden("Request Not allowed due to Invalid token!")
          }
        case None =>
          logger.warn("Header not present. Invalid Request !!")
          Http.forbidden("Request Not allowed. Header not present!")
      }
    })
  }

  private [etlflow] def login(args: UserArgs): RIO[DBServerEnv with CacheEnv, UserAuth] =  {
    DBServerApi.getUser(args.user_name).foldM(ex => {
      logger.error("Error in fetching user from db => " + ex.getMessage)
      Task(UserAuth("Invalid User/Password", ""))
    }, user => {
      if (BCrypt.checkpw(args.password, user.password)) {
        val user_data = s"""{"user":"${user.user_name}", "role":"${user.user_role}"}""".stripMargin
        val token = Jwt.encode(user_data, secret, JwtAlgorithm.HS256)
        logger.info(s"New token generated for user ${user.user_name}")
        CacheApi.put(cache, token, token, Some(default_ttl)).as(UserAuth("Valid User", token))
      } else {
        Task(UserAuth("Invalid User/Password", ""))
      }
    })
  }
}
