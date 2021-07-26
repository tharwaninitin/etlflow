package etlflow.coretests.utils

import com.github.t3hnar.bcrypt._
import etlflow.utils.EncryptionAPI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class EncryptTestSuite  extends AnyFlatSpec with should.Matchers {
  "Encrypt should " should "retrieve correct value" in {
    assert(EncryptionAPI.encrypt("admin") == "twV4rChhxs76Z+gY868NSw==")
  }

  val password = EncryptionAPI.encryptKey("abc")
  "Symmetric Encrypt key should " should "retrieve correct value" in {
    assert(password.isBcryptedBounded(password) == false)
  }
}
