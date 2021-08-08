package etlflow.gcp

import zio.Task
import scala.reflect.runtime.universe._

private[etlflow] object ReflectAPI {

  def getFields[T: TypeTag]: Task[Seq[(String, String)]] = Task {
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
    }.toSeq
  }
}
