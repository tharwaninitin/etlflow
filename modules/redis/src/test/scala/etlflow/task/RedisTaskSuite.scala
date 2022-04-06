package etlflow.task

import RedisTask.RedisCmd
import etlflow.model.Credential.REDIS
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object RedisTaskSuite extends DefaultRunnableSpec {

  val redisConfig: REDIS = REDIS("localhost")

  private val task1 = RedisTask(
    name = "set_redis_key_value_1",
    command = RedisCmd.SET(Map("key1" -> "value1", "key2" -> "value3", "key3" -> "value3")),
    credentials = redisConfig
  )

  private val task2 = RedisTask(
    name = "set_redis_key_value_2",
    command = RedisCmd.SET(Map("key4" -> "value4", "key5" -> "value5", "key6" -> "value6")),
    credentials = redisConfig
  )

  private val task3 = RedisTask(
    name = "delete_keys_from_redis",
    command = RedisCmd.DELETE(List("*key1*")),
    credentials = redisConfig
  )

  private val task4 = RedisTask(
    name = "flushall_keys_from_redis",
    command = RedisCmd.FLUSHALL,
    credentials = redisConfig
  )

  private val task5 = RedisTask(
    name = "delete_none_from_redis",
    command = RedisCmd.DELETE(List("*key1*")),
    credentials = redisConfig
  )

  private val job = for {
    _ <- task1.executeZio
    _ <- task2.executeZio
    _ <- task3.executeZio
    _ <- task4.executeZio
    _ <- task5.executeZio
  } yield ()

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Redis Tasks")(testM("Execute redis tasks") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }).provideCustomLayerShared(etlflow.log.noLog)
}
