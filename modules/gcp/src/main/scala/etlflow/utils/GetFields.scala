package etlflow.utils

import zio.Tag

object GetFields {
  def apply[T: Tag]: Array[(String, String)] = {
    implicitly[Tag[T]].closestClass.getDeclaredFields.map(f => {
      (f.getName,f.getType.getName)
    })
  }
}
