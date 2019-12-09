package etljobs.functions

trait BusinessUDF {
  def parser(args: Array[String]): Map[String, String] = {
    args.map {
      case arg => {
        val keyValue = arg.split("==");
        keyValue(0) -> keyValue(1)
      }
    }.toMap
  }

}
