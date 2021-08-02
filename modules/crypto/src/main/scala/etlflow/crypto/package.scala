package etlflow

import zio.Has

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

package object crypto {

  type CryptoEnv = Has[CryptoApi.Service]
  final val secretKey = "enIntVecTest2020"
  final val iv = new IvParameterSpec(secretKey.getBytes("UTF-8"))
  final val skeySpec = new SecretKeySpec(secretKey.getBytes("UTF-8"), "AES")
  final val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")

}
