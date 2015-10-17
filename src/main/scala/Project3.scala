import akka.actor.Actor
import java.security.MessageDigest
import akka.actor.ActorSystem
import akka.actor.Props

object Project2 {

  val nodePrefix = "Node-"

  def getName(id: String, offSet: Int = 16): String = {
    if (id != null) {
      var key = MessageDigest.getInstance("SHA-128").digest(id.getBytes("UTF-8")).toString()
      if (key.length() > offSet) {
        key = key.substring(key.length() - offSet);
      }
      key
    }
    id
  }

  def main(args: Array[String]): Unit = {

    var numNodes = 5;
    var numRequests = 10;

    if (args.length > 0) {
      numNodes = args(0).toInt
      numRequests = args(1).toInt
    }

    val system = ActorSystem("ChordSys")

    var roundUp = (2 ^ 10) * (math.log10(numNodes) / math.log10(2));

    var firstNode: String = null;

    for (i <- 1 to numNodes) {

      if (firstNode == null) {
        firstNode = getName(nodePrefix + i)
        var a = system.actorOf(Props[Peer], firstNode)
        // Initialize the first node
        a ! new bootPeer(null)
      } else {
        // Use the firstNode as the neighbor to lookup information.
      }
    }

  }
}

sealed trait Seal
case class bootPeer(myRandNeigh: String) extends Seal

class Peer(val id: String) extends Actor {

  def receive = {
    case b: bootPeer => {
      if (b.myRandNeigh == null) {

      } else {

      }
    }
  }
}