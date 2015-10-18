

import java.security.MessageDigest
import java.lang.Long

object TEST extends App {

  var x = MessageDigest.getInstance("SHA-256").digest("tarun1".getBytes("UTF-8")).map("%02X" format _).mkString.trim()
  x = x.substring(x.length() - 16)  
  x = "FFFFFFFFFFFFFFF"
  var t:Int = 2000000000
  print(t + " - ")
  
  print(Int.MaxValue)
  print(Integer.MAX_VALUE)
  
}