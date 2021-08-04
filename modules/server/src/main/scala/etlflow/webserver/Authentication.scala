package etlflow.webserver

import com.github.t3hnar.bcrypt._
import etlflow.api.Schema.{UserArgs, UserAuth}
import etlflow.cache.{CacheApi, CacheEnv, default_ttl}
import etlflow.db.{DBApi, DBEnv}
import etlflow.schema.WebServer
import etlflow.utils.ApplicationLogger
import pdi.jwt.{Jwt, JwtAlgorithm}
import scalacache.caffeine.CaffeineCache
import zhttp.http.{HttpApp, _}
import zio.Runtime.default.unsafeRun
import zio.{RIO, Task, ZEnv, ZIO}

case class Authentication(cache: CaffeineCache[String], config: Option[WebServer]) extends  ApplicationLogger {
  final val secret = config.map(_.secretKey.getOrElse("secretKey")).getOrElse("secretKey")
  private [etlflow] def validateJwt(token: String): Boolean = Jwt.isValid(token, secret, Seq(JwtAlgorithm.HS256))

  private [etlflow] def isCached(token: String): ZIO[ZEnv, Throwable, Option[String]] = CacheApi.getKey(cache, token).provideCustomLayer(etlflow.cache.Implementation.live)

  private [etlflow] def middleware[R, E](app: HttpApp[R, E]): HttpApp[R, E] =  Http.flatten {
    Http.fromFunction[Request](req => {
      val headerValue = if(req.getHeader("Authorization").isDefined)
        req.getHeader("Authorization")
      else
        req.getHeader("X-Auth-Token")
      headerValue match {
        case Some(value) =>
          val token = value.value.toString
          if(validateJwt(token)) {
            unsafeRun(isCached(token)) match {
              case Some(_) => app
              case None =>
                logger.warn(s"Expired token $token")
                HttpApp.forbidden("Not allowed!")
            }
          }
          else {
            logger.warn(s"Invalid token $token")
            HttpApp.forbidden("Request Not allowed due to Invalid token!")
          }
        case None =>
          logger.warn("Header not present. Invalid Request !!")
          HttpApp.forbidden("Request Not allowed. Header not present!")
      }
    })
  }

  private [etlflow] def login(args: UserArgs): RIO[DBEnv with CacheEnv, UserAuth] =  {
    DBApi.getUser(args.user_name).foldM(ex => {
      logger.error("Error in fetching user from db => " + ex.getMessage)
      Task(UserAuth("Invalid User/Password", ""))
    }, user => {
      if (args.password.isBcryptedBounded(user.password)) {
        val user_data = s"""{"user":"${user.user_name}", "role":"${user.user_role}"}""".stripMargin
        val token = Jwt.encode(user_data, secret, JwtAlgorithm.HS256)
        logger.info(s"New token generated for user ${user.user_name}")
        CacheApi.putKey(cache, token, token, Some(default_ttl)).map( _ => UserAuth("Valid User", token))
      } else {
        Task(UserAuth("Invalid User/Password", ""))
      }
    })
  }
}
