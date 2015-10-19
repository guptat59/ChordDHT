import akka.actor.Actor
import java.security.MessageDigest
import akka.actor.ActorSystem
import akka.actor.Props
import java.lang.Long

object Project2 {

  val nodePrefix = "Node-"
  val totalSpace: Int = Integer.MAX_VALUE

  def getHash(id: String, totalSpace: Long): Int = {

    // Maximum total hash space can be 2 ^ 60.
    if (id != null) {
      var key = MessageDigest.getInstance("SHA-256").digest(id.getBytes("UTF-8")).map("%02X" format _).mkString.trim()
      if (key.length() > 15) {
        key = key.substring(key.length() - 15);
      }
      (Long.parseLong(key, 16) % totalSpace).toInt
    }
    0
  }

  def main(args: Array[String]): Unit = {

    var numNodes = 5;
    var numRequests = 10;

    if (args.length > 0) {
      numNodes = args(0).toInt
      numRequests = args(1).toInt
    }

    val system = ActorSystem("ChordSys")

    // totalSpace = ((math.ceil((math.log10(numNodes) / math.log10(2)))).toInt) * (2 ^ 10);    
    var firstNode: Int = -1;

    for (i <- 1 to numNodes) {

      if (firstNode == -1) {
        firstNode = getHash(nodePrefix + i, totalSpace)
        var a = system.actorOf(Props(new Peer(firstNode, nodePrefix + i)), firstNode.toString())
        // Initialize the first node
        a ! new bootPeer(null)
      } else {
        // Use the firstNode as the neighbor to lookup information.
        var hashName = getHash(nodePrefix + i, totalSpace)
        var a = system.actorOf(Props(new Peer(hashName, nodePrefix + i)), hashName.toString())
        a ! new bootPeer(firstNode)
      }
    }
  }
}

sealed trait Seal
case class bootPeer(myRandNeigh: Long) extends Seal
case class hasFileKey(fileKey: String) extends Seal

class Peer(val hashName: Int, val abstractName: String) extends Actor {

  var range1: Range = null;
  var range2: Range = null;

  def receive = {
    case b: bootPeer => {
      if (b.myRandNeigh == null) {
        // Implies first Node.
        range1 = 3 to Project2.totalSpace + 1
        range2 = Project2.totalSpace to hashName
      } else {
        // nth node.
      }
    }
    
    

    case h: hasFileKey => {
      var found = false;      
      if (range1 != null) {
        found = range1.contains(h.fileKey)
        if (!found) {
          if (range2 != null) found = range2.contains(h.fileKey)
        }
      }
      if (found) {
        println("Found key " + h.fileKey + " at node " + context.self.path.name)
      }
    }

  }
}