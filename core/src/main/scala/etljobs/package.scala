package object etljobs {
  trait EtlJobName
  trait EtlProps
  case class EtlJobException(msg : String) extends Exception
}
