package etlflow.etlsteps

import com.redis._
import etlflow.etlsteps.RedisStep.RedisCmd
import etlflow.redis.RedisApi
import etlflow.model.Credential.REDIS
import zio.Task

case class RedisStep(name: String, command: RedisCmd, credentials: REDIS) extends EtlStep[Unit] {
  override protected type R = Any

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  override protected def process: Task[Unit] = Task {
    logger.info("#" * 100)
    val redisClient = new RedisClient(credentials.host_name, credentials.port, secret = credentials.password)
    logger.info(s"Starting Redis Query Step: $name")
    logger.info(s"Query to perform: $command")
    command match {
      case RedisCmd.SET(kv)        => RedisApi.setKeys(kv, redisClient)
      case RedisCmd.FLUSHALL       => redisClient.flushall; ()
      case RedisCmd.DELETE(prefix) => RedisApi.deleteKeysOfPreFix(prefix, redisClient)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getStepProperties: Map[String, String] = Map("operation_type" -> command.toString)
}

object RedisStep {
  sealed trait RedisCmd
  object RedisCmd {
    final case class SET(kv: Map[String, String]) extends RedisCmd
    final case object FLUSHALL                    extends RedisCmd
    final case class DELETE(prefix: List[String]) extends RedisCmd
  }
}
