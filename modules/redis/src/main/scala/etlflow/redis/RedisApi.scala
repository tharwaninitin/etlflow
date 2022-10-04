package etlflow.redis

import com.redis.RedisClient
import etlflow.utils.ApplicationLogger

private[etlflow] object RedisApi extends ApplicationLogger {

  def setKeys(prefix: Map[String, String], redisClient: RedisClient): Unit =
    prefix.foreach { value =>
      logger.info(s"Redis key_value - ${value._1}_${value._2}")
      redisClient.set(value._1, value._2)
    }

  private def getKeysFromPreFix(name: String, redisClient: RedisClient): Option[List[Option[String]]] = {
    logger.info(s"Redis keys for prefix - $name are : ${redisClient.keys(name)}")
    redisClient.keys(name)
  }

  private def enrichKeys(keys: Option[List[Option[String]]]): List[String] =
    keys match {
      case Some(value) => if (value.isEmpty) List.empty else value.map(value => value.getOrElse(""))
      case None        => List.empty
    }

  @SuppressWarnings(
    Array("org.wartremover.warts.TraversableOps", "org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.IterableOps")
  )
  def deleteKeysOfPreFix(prefix: List[String], redisClient: RedisClient): Unit =
    prefix.foreach { value =>
      val keys         = getKeysFromPreFix(value, redisClient)
      val enrichedKeys = enrichKeys(keys)
      logger.info(s"Redis keys for prefix - $value are : $enrichedKeys")
      if (enrichedKeys.nonEmpty) redisClient.del(enrichedKeys.head, enrichedKeys.tail: _*)
      logger.info(s"Redis keys are deleted for prefix - $value")
    }
}
