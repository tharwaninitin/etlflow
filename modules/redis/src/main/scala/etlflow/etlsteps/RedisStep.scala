package etlflow.etlsteps

import com.redis._
import etlflow.etlsteps.RedisStep.RedisCmd
import etlflow.schema.Credential.REDIS
import etlflow.schema.LoggingLevel
import zio.Task

class RedisStep (
                  val name: String,
                  val command: RedisCmd,
                  val credentials: REDIS
                )
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    logger.info("#" * 100)
    val redisClient = new RedisClient(credentials.host_name, credentials.port, secret=credentials.password)
    logger.info(s"Starting Redis Query Step: $name")
    logger.info(s"Query to perform: $command")
    command match {
      case RedisCmd.SET(kv) => Task(setKeys(kv,redisClient))
      case RedisCmd.FLUSHALL => Task(redisClient.flushall)
      case RedisCmd.DELETE(prefix) => Task(deleteKeysOfPreFix(prefix,redisClient))
    }
  }

  private def getKeysFromPreFix(name:String, redisClient: RedisClient) : Option[List[Option[String]]] = {
    logger.info(s"Redis keys for prefix - $name are : " + redisClient.keys(name))
    redisClient.keys(name)
  }

  private def setKeys(prefix:Map[String,String], redisClient: RedisClient):Unit = {
    prefix.foreach {
      value =>
        logger.info(s"Redis key_value - ${value._1}_${value._2}")
        redisClient.set(value._1,value._2)
    }
  }

  private def deleteKeysOfPreFix(prefix:List[String], redisClient: RedisClient):Unit = {
    prefix.foreach {
      value =>
        val keys = getKeysFromPreFix(value,redisClient)
        val enrichedKeys = enrichKeys(keys)
        logger.info(s"Redis enriched keys for prefix - $name are : " + enrichedKeys)
        redisClient.del( enrichedKeys.head,enrichedKeys.tail:_*)
        logger.info(s"Redis keys are deleted for prefix - $name")
    }
  }

  private def enrichKeys(keys:Option[List[Option[String]]]) = {
    keys match {
      case Some(key) => if(key.isEmpty) List(None,None) else key.map(value => value.get)
      case None => List(None,None)
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("operation_type" -> command.toString)
}

object RedisStep {
  sealed trait RedisCmd
  object RedisCmd {
    case class SET(kv: Map[String,String]) extends RedisCmd
    case object FLUSHALL extends RedisCmd
    case class DELETE(prefix: List[String]) extends RedisCmd
  }

  def apply(name: String, command: RedisCmd, credentials: REDIS): RedisStep = new RedisStep(name, command, credentials)
}
