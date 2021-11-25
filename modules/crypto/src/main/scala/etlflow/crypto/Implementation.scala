package etlflow.crypto

import org.mindrot.jbcrypt.BCrypt
import zio.{Task, ULayer, ZLayer}
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

object Implementation {

  def live(key: Option[String]): ULayer[CryptoEnv] = ZLayer.succeed(
    new CryptoApi.Service {

      final val secretKey = key.getOrElse("EtlFlowCrypt2020")
      final val iv = new IvParameterSpec(secretKey.getBytes("UTF-8"))
      final val skeySpec = new SecretKeySpec(secretKey.getBytes("UTF-8"), "AES")
      final val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")

      override def encrypt(text: String): Task[String] = Task{
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
        val encrypted = cipher.doFinal(text.getBytes())
        Base64.getEncoder.encodeToString(encrypted)
      }

      override def decrypt(text: String): Task[String] = Task{
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
        val decrypted = cipher.doFinal(Base64.getDecoder.decode(text))
        new String(decrypted)
      }

      override def oneWayEncrypt(text: String, salt: Option[Int] = None): Task[String] = Task{
        val log_rounds = BCrypt.gensalt(salt.getOrElse(10))
        BCrypt.hashpw(text, log_rounds)
      }
    }
  )
}

