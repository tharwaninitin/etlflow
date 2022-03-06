package etlflow.etlsteps

import etlflow.etlsteps.RedisStep.RedisCmd
import etlflow.model.Credential.REDIS
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object RedisStepSuite extends DefaultRunnableSpec {

  val redis_config: REDIS = REDIS("localhost")

  val step1 = RedisStep(
    name = "set_redis_key_value_1",
    command = RedisCmd.SET(Map("key1" -> "value1", "key2" -> "value3", "key3" -> "value3")),
    credentials = redis_config
  )

  val step2 = RedisStep(
    name = "set_redis_key_value_2",
    command = RedisCmd.SET(Map("key4" -> "value4", "key5" -> "value5", "key6" -> "value6")),
    credentials = redis_config
  )

  val step3 = RedisStep(
    name = "delete_keys_from_redis",
    command = RedisCmd.DELETE(List("*key1*")),
    credentials = redis_config
  )

  val step4 = RedisStep(
    name = "flushall_keys_from_redis",
    command = RedisCmd.FLUSHALL,
    credentials = redis_config
  )

  val step5 = RedisStep(
    name = "delete_none_from_redis",
    command = RedisCmd.DELETE(List("*key1*")),
    credentials = redis_config
  )

  val job = for {
    _ <- step1.execute
    _ <- step2.execute
    _ <- step3.execute
    _ <- step4.execute
    _ <- step5.execute
  } yield ()

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Redis Steps")(testM("Execute redis steps") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }).provideCustomLayerShared(etlflow.log.nolog)
}
