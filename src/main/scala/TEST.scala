

import java.security.MessageDigest
import java.lang.Long
import scala.collection.mutable.ListMap
import java.util.HashMap

object TEST extends App {
  
  var map = new HashMap[Integer, String]();
  //var map = new HashMap<Integer,Integer>()
  //var map = new ListMap[Int, Int]
 /* map(2345) = 123456
  map(121) = 765
  map(12312312) = 1342353466
  println(map)
  map = map.toSeq.sortBy(_._1):_**/
  println(map)

  var totalSpace = ((math.ceil((math.log10(Integer.MAX_VALUE) / math.log10(2)))).toInt);
  println(totalSpace)
  var x = MessageDigest.getInstance("SHA-256").digest("tarun1".getBytes("UTF-8")).map("%02X" format _).mkString.trim()
  x = x.substring(x.length() - 16)
  x = "FFFFFFFFFFFFFFF"
  var t: Int = 2000000000
  print(t + " - ")

  print(Int.MaxValue)
  print(Integer.MAX_VALUE)

}