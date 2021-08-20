package etlflow.gcp

import scala.reflect.runtime.universe._

private[etlflow] object ReflectAPI {

  def getFields[T: TypeTag]: Seq[(String, String)] = 
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
    }.toSeq
}
