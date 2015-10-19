import akka.actor.Actor
import java.security.MessageDigest
import akka.actor.ActorSystem
import akka.actor.Props
import java.lang.Long
import javafx.scene.paint.Stop
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import akka.actor.ActorRef
import scala.concurrent.impl.Future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import akka.event.Logging
import com.typesafe.config.ConfigFactory

object Constants {
  val nodePrefix = "Node-"
  val m: Int = (math.ceil((math.log10(Integer.MAX_VALUE) / math.log10(2)))).toInt - 1
  val totalSpace: Int = Math.pow(2, m).toInt
  val actorSystem = "ChordSys"
  val namingPrefix = "akka://" + actorSystem + "/user/"
}

object Chord {

  def getHash(id: String, totalSpace: Int): Int = {

    // Maximum total hash space can be 2 ^ 30.
    if (id != null) {
      var key = MessageDigest.getInstance("SHA-256").digest(id.getBytes("UTF-8")).map("%02X" format _).mkString.trim()
      if (key.length() > 15) {
        key = key.substring(key.length() - 15);
      }
      (Long.parseLong(key, 16) % totalSpace).toInt
    } else
      0
  }

  def main(args: Array[String]): Unit = {

    var numNodes = 2;
    var numRequests = 10;

    if (args.length > 0) {
      numNodes = args(0).toInt
      numRequests = args(1).toInt
    }

    var config = """
      akka {
          loglevel = DEBUG
      }"""
    val system = ActorSystem(Constants.actorSystem, ConfigFactory.parseString(config))

    // totalSpace = ((math.ceil((math.log10(numNodes) / math.log10(2)))).toInt) * (2 ^ 10);    
    var firstNode: Int = -1;
    var node_1: ActorRef = null;
    for (i <- 1 to numNodes) {

      if (firstNode == -1) {
        // Initialize the first node
        firstNode = getHash(Constants.nodePrefix + i, Constants.totalSpace)
        node_1 = system.actorOf(Props(new Peer(firstNode, Constants.nodePrefix + i)), firstNode.toString())
        node_1 ! new join(-1)
        println("First Node" + firstNode)
      } else {
        // Use the firstNode as the neighbor to lookup information.
        var hashName = getHash(Constants.nodePrefix + i, Constants.totalSpace)
        var node = system.actorOf(Props(new Peer(hashName, Constants.nodePrefix + i)), hashName.toString())
        node ! new join(firstNode)
        Thread.sleep(1000)
      }

    }
    node_1 ! "print"
  }
}

sealed trait Seal
case class join(myRandNeigh: Int) extends Seal
case class initFingerTable() extends Seal
case class hasFileKey(fileKey: String) extends Seal
case class getPredec() extends Seal
case class getSucc(fromNewBie: Boolean) extends Seal
case class setPredec(name: Int, fromNewbie: Boolean) extends Seal
case class setSucc(name: Int, fromNewbie: Boolean) extends Seal
case class findPredecessor(key: Int, origin: Int, isRequest: Boolean) extends Seal
case class findSuccessor(key: Int, origin: Int, Type: String) extends Seal
case class Finger(hashKey: Int, node: Int, Type: String)

class Peer(val hashName: Int, val abstractName: String) extends Actor {

  object Consts {
    val fingerRequest = "fingerRequest"
  }

  val log = Logging(context.system, this)
  // var range1: Range = null;
  // var range2: Range = null;
  var finger = new HashMap[Int, Int]
  var successor: Int = -1;
  var predecessor: Int = -1;
  var isJoined: Boolean = false;
  def receive = {
    case b: join => {

      if (b.myRandNeigh == -1) {
        // Implies first Node.
        //range1 = hashName to Project2.totalSpace - 1
        //range2 = 0 to hashName-1
        successor = hashName;
        predecessor = hashName;
        fillFingerTable(-1);
        isJoined = true
      } else {
        var act = context.actorSelection(Constants.namingPrefix + b.myRandNeigh)
        act ! findPredecessor(hashName, hashName, true)       
        // predecessor = findPredecessor(b.myRandNeigh)
        // successor = findSuccessor(b.myRandNeigh)
        //  fillFingerTable(predecessor)
        // updateFingerTable(hashName);

        //actorRef.successor = hashName; setting the successor to new node.
        // fillFingerTable(hashName)
      }
    }
    case f: initFingerTable => {
      var prevStart: Int = (hashName + Math.pow(2, 0).toInt) % Constants.totalSpace
      finger(prevStart) = successor
      var loopOnce: Boolean = false
      for (i <- 1 to Constants.m - 1) {
        var start: Int = (hashName + Math.pow(2, i).toInt) % Constants.totalSpace
        finger(start) = -1
        if (start < finger(prevStart)) {
          finger(start) = finger(prevStart)
        } else if (!loopOnce) {
          loopOnce = !loopOnce
          var sucActor = context.actorSelection(Constants.namingPrefix + successor)
          sucActor ! findSuccessor(start, hashName, Consts.fingerRequest)
        }
        prevStart = start
      }
      //println(finger)
    }

    case a: Finger => {
      if (a.equals(Consts.fingerRequest)) {
        finger(a.hashKey) = a.node
      }
    }
    case h: hasFileKey => {

      /*var found = false;      
      if (range1 != null) {
        found = range1.contains(h.fileKey)
        if (!found) {
          if (range2 != null) found = range2.contains(h.fileKey)
        }
      }
      if (found) {
        println("Found key " + h.fileKey + " at node " + context.self.path.name)
      }
    }*/
    }

    case s: getSucc => {
      log.debug("\tType:getSucc\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + s)
      if (s.fromNewBie) {
        sender ! setSucc(successor, false)
      }
    }

    case p: getPredec => {

    }

    case s: setSucc => {
      log.debug("\tType:setSucc\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + s)
      if (s.fromNewbie && isJoined) {
        var myOldSuccesor = context.actorSelection(Constants.namingPrefix + successor)
        successor = s.name
        myOldSuccesor ! setPredec(s.name, isJoined)
      } else if (!s.fromNewbie && !isJoined) {
        successor = s.name
        //Since I am a new node, Update the successor info of my new predecessor
        var myNewPredecessor = context.actorSelection(Constants.namingPrefix + predecessor)
        myNewPredecessor ! setSucc(hashName, !isJoined)
        var f = Future {
          Thread.sleep(10)
        }
        //f.onComplete { case x => self.tell(initFingerTable(), self) } //println("me" + hashName + " pre: " + predecessor + " Succ: " + successor
        //f.onComplete { case x => self.tell("print", self) }

      }
    }

    case p: setPredec => {
      log.debug("\tType:setPredec\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + p)
      if (isJoined && !p.fromNewbie) {
        predecessor = p.name;
      } else if (!isJoined && !p.fromNewbie) {
        predecessor = p.name
        var act = context.actorSelection(Constants.namingPrefix + predecessor)
        act ! getSucc(!isJoined)
      }
    }

    case s: findSuccessor => {
      if (s.Type.equals(Consts.fingerRequest)) {
        log.debug("\tType:findSuccessor\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + s)
        var start = hashName
        var end = successor
        if (s.key >= start && s.key < end) {
          var act = context.actorSelection(Constants.namingPrefix + s.origin)
          act ! Finger(s.key, end, s.Type)
        } else {
          var closetNeigh = closestPrecedingFinger(s.key);
          var act = context.actorSelection(Constants.namingPrefix + closetNeigh)
          act ! findSuccessor(s.key, s.origin, s.Type)
        }
      }
    }

    case f: findPredecessor => {
      if (f.isRequest) {
        log.debug("\tType:findPredecessor\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + f)
        var start = hashName
        var end = successor
        if (start == end) {
          var act = context.actorSelection(Constants.namingPrefix + f.origin)
          act ! setPredec(start, false)
        } else if ((f.key >= start && f.key < end) || ((f.key >= start && f.key < Constants.totalSpace) || (f.key >= 0 && f.key < end))) {
          var act = context.actorSelection(Constants.namingPrefix + f.origin)
          act ! setPredec(start, false)
        } else {
          var closetNeigh = closestPrecedingFinger(f.key);
          var act = context.actorSelection(Constants.namingPrefix + closetNeigh)
          act ! findPredecessor(f.key, f.origin, f.isRequest)
        }
      }
    }

    case x: String => {
      if (x.equals("print")) {
        println("My name " + hashName + " Pre: " + predecessor + " Succ: " + successor)
      }
    }

  }

  //  def fillFingerTable(myRandNeigh: Int, hashName: Int): Unit = {
  //    if (myRandNeigh == -1) {
  //      for (i <- 0 until Chord.m ) {
  //        //range = (hashName + (Math.pow(2, i).toInt % Chord.totalSpace).toInt) to (hashName + (Math.pow(2, i + 1).toInt % Chord.totalSpace).toInt)
  //        fingerTable(i) = hashName
  //      }
  //    } else {
  //      //update
  //    }
  //  }

  def fillFingerTable(myRandNeigh: Int): Unit = {
    if (myRandNeigh == -1) {
      for (i <- 0 until Constants.m) {
        finger(hashName + (Math.pow(2, i).toInt % Constants.totalSpace).toInt) = hashName
      }
      //print(finger)
    } else {
      //update
    }
  }

  def findPredec(key: Int): Int = {
    1
  }

  def findSucc(myRandNeigh: Int): Int = {
    // conext.A
    1
  }

  def updateFingerTable(hashName: Int): Int = {
    1
  }

  def closestPrecedingFinger(key: Int): Int = {
    var keyFound = Integer.MIN_VALUE
    var it = finger.keys.iterator
    while (it.hasNext) {
      var current = it.next().toInt
      if (current < key) {
        if (keyFound < current) {
          keyFound = current
        }
      }
    }
    keyFound
  }
}