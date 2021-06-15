package etlflow.webserver

import com.github.t3hnar.bcrypt._
import etlflow.api.Schema.{UserArgs, UserAuth}
import etlflow.jdbc.{DB, DBEnv}
import etlflow.log.ApplicationLogger
import etlflow.schema.WebServer
import etlflow.utils.CacheHelper
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim}
import scalacache.caffeine.CaffeineCache
import zhttp.http.{HttpApp, _}
import zio.RIO

case class Authentication(cache: CaffeineCache[String], config: Option[WebServer]) extends  ApplicationLogger {
  final val secret = config.map(_.secretKey.getOrElse("secretKey")).getOrElse("secretKey")
  def validateJwt(token: String): Boolean = Jwt.isValid(token, secret, Seq(JwtAlgorithm.HS256))

  def jwtDecode(token: String): Option[JwtClaim] = {
    Jwt.decode(token, secret, Seq(JwtAlgorithm.HS512)).toOption
  }
  def isCached(token: String): Option[String] = CacheHelper.getKey(cache, token)

  def middleware[R, E](app: HttpApp[R, E]): HttpApp[R, E] =  Http.flatten {
    Http.fromFunction[Request](req => {
      val headerValue = if(req.getHeader("Authorization").isDefined)
        req.getHeader("Authorization")
      else
        req.getHeader("X-Auth-Token")
      headerValue match {
        case Some(value) =>
          val token = value.value.toString
          if(validateJwt(token)) {
            isCached(token) match {
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


  def login(args: UserArgs): RIO[DBEnv, UserAuth] =  {
    DB.getUser(args.user_name).fold(ex => {
      logger.error("Error in fetching user from db => " + ex.getMessage)
      UserAuth("Invalid User/Password", "")
    }, user => {
      if (args.password.isBcryptedBounded(user.password)) {
        val user_data = s"""{"user":"${user.user_name}", "role":"${user.user_role}"}""".stripMargin
        val token = Jwt.encode(user_data, secret, JwtAlgorithm.HS256)
        logger.info(s"New token generated for user ${user.user_name}")
        CacheHelper.putKey(cache, token, token, Some(CacheHelper.default_ttl))
        UserAuth("Valid User", token)
      } else {
        UserAuth("Invalid User/Password", "")
      }
    })
  }
}
