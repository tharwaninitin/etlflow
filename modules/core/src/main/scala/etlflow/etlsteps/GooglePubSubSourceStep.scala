package etlflow.etlsteps

import cats.effect.Blocker
import com.google.pubsub.v1.PubsubMessage
import com.permutive.pubsub.consumer.{ConsumerRecord, Model}
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import zio.Task
import zio.interop.catz._

case class GooglePubSubSourceStep[T: MessageDecoder](
       name: String,
       subscription: String,
       project_id: String,
       error_handler: (PubsubMessage, Throwable, Task[Unit], Task[Unit]) => Task[Unit] =
          (msg, err, ack, _) => Task(println(s"Got error $err")), //*> ack,
       success_handler: ConsumerRecord[Task,T] => Task[Unit],
       limit: Option[Int] = None
     )
  extends EtlStep[Unit,Unit] {

  final def process(input: => Unit): Task[Unit] = {
    Task.concurrentEffectWith { implicit CE =>
      Blocker[Task].use { blocker =>

        etl_logger.info("#"*50)
        etl_logger.info(s"Starting Pub Sub Step: $name")

        val stream = PubsubGoogleConsumer.subscribe[Task, T](
          blocker,
          Model.ProjectId(project_id),
          Model.Subscription(subscription),
          error_handler,
          config = PubsubGoogleConsumerConfig(
            onFailedTerminate = _ => Task.unit
          )
        )

        limit.map{n =>
          stream
            .evalTap(success_handler)
            .take(n)
            .compile
            .drain
        }.getOrElse(
          stream
            .evalTap(success_handler)
            .compile
            .drain
        )
      }
    } *> Task(etl_logger.info("#"*50))
  }
}