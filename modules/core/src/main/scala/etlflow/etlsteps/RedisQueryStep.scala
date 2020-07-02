package etlflow.etlsteps

import com.redis._
import etlflow.utils.REDIS
import zio.Task

class RedisQueryStep (
                    val name: String,
                    val query: String,
                    val prefix: Option[String] = None,
                    val credentials :REDIS

                  )
  extends EtlStep[Unit,Unit] {

  val redisClient = new RedisClient(credentials.user, credentials.port,secret=Some(credentials.password))

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting Redis Query Step: $name")
    etl_logger.info(s"Query to perform: $query")
    query  match {
      case "flushall" => Task(redisClient.flushall)
      case "set" => Task(redisClient.set(name,name))
      case "delete" => {
          val name = prefix
          val keys = redisClient.keys(name)
          etl_logger.info("KEYS ARE : " + name)
          Task(redisClient.del(keys))
      }
    }
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> query)
}

object RedisQueryStep {
  def apply(name: String, query: String,prefix: Option[String]=None,credentials :REDIS): RedisQueryStep =
    new RedisQueryStep(name, query,prefix,credentials)
}
