package etlflow.etlsteps

import com.redis._
import etlflow.utils.REDIS
import zio.Task
import etlflow.utils.{HttpClientApi, JsonJackson, LoggingLevel}


class RedisStep (
                  val name: String,
                  val operation_type: String,
                  val kv:Map[String,String] = Map.empty,
                  val prefix: List[String] = List.empty,
                  val credentials :REDIS

                )
  extends EtlStep[Unit,Unit] {

  val redisClient = new RedisClient(credentials.host_name, credentials.port,secret=credentials.password)

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#" * 100)
    etl_logger.info(s"Starting Redis Query Step: $name")
    etl_logger.info(s"Query to perform: $operation_type")
    operation_type match {
      case "set" => Task(setKeys(kv))
      case "flushall" => Task(redisClient.flushall)
      case "delete" => Task(deleteKeysOfPreFix(prefix))
    }
  }


  def getKeysFromPreFix(name:String) : Option[List[Option[String]]] = {
    etl_logger.info(s"Redis keys for prefix - $name are : " + redisClient.keys(name))
    redisClient.keys(name)
  }

  def setKeys(prefix:Map[String,String]):Unit = {
    prefix.foreach {
      value =>
        etl_logger.info(s"Redis key_value - ${value._1}_${value._2}")
        redisClient.set(value._1,value._2)
    }
  }

  def deleteKeysOfPreFix(prefix:List[String]):Unit = {
    prefix.foreach {
      value =>
        val keys = getKeysFromPreFix(value)
        val enrichedKeys = enrichKeys(keys)
        etl_logger.info(s"Redis enriched keys for prefix - $name are : " + enrichedKeys)
        redisClient.del( enrichedKeys.head,enrichedKeys.tail:_*)
        etl_logger.info(s"Redis keys are deleted for prefix - $name")
    }

  }

  private def enrichKeys(keys:Option[List[Option[String]]]) = {
    keys match {
      case Some(key) => if(key.isEmpty) List(None,None) else key.map(value => value.get)
      case None => List(None,None)
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("query" -> operation_type)
}

object RedisStep {
  def apply(name: String, operation_type: String, kv:Map[String,String] = Map.empty,prefix: List[String] = List.empty,credentials :REDIS): RedisStep =
    new RedisStep(name, operation_type,kv,prefix,credentials)
}
