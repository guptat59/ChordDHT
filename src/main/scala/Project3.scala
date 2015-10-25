import akka.actor.Actor
import java.security.MessageDigest
import akka.actor.ActorSystem
import akka.actor.Props
import java.lang.Long
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import akka.actor.ActorRef
import scala.concurrent.impl.Future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import akka.event.Logging
import com.typesafe.config.ConfigFactory
import java.util.NoSuchElementException
import com.sun.xml.internal.fastinfoset.tools.PrintTable
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.decorateAsScala._
import scala.collection._

object Constants {
  val nodePrefix = "Node-"
  val m: Int = (math.ceil((math.log10(Integer.MAX_VALUE) / math.log10(2)))).toInt - 1
  val totalSpace: Int = Math.pow(2, m).toInt
  val actorSystem = "ChordSys"
  val namingPrefix = "akka://" + actorSystem + "/user/"
  val fingerRequest = "fingerRequest"
  val setRequest = "setRequest"
  val fingerUpdate = "fingerUpdate"
  val fileSearch = "fileSearch"
}

object Chord {

  var nodesJoined: Int = 0;
  //var fileFound = new HashMap[Int, Int]
  var fileFound: concurrent.Map[Int, Int] = new ConcurrentHashMap().asScala
  var TotalNumofHops = 0;
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

  var config = """
      akka {
          loglevel = INFO
      }"""

  def main(args: Array[String]): Unit = {

    var numNodes = 64;
    var numRequests = 10;
    var avgHopsinSystem = 0;
    if (args.length > 0) {
      numNodes = args(0).toInt
      numRequests = args(1).toInt
    }

    val system = ActorSystem(Constants.actorSystem, ConfigFactory.parseString(config))

    var firstNode: Int = -1;
    var node_1: ActorRef = null;
    for (i <- 1 to numNodes) {

      if (firstNode == -1) {
        // Initialize the first node
        firstNode = getHash(Constants.nodePrefix + i, Constants.totalSpace)
        println("Initializing the first peer with hash Id " + firstNode)
        node_1 = system.actorOf(Props(new Peer(firstNode, Constants.nodePrefix + i, numRequests)), firstNode.toString())
        node_1 ! new join(-1)
        Thread.sleep(1000)
        println("First Node" + firstNode)
      } else {
        Thread.sleep(100)
        // Use the firstNode as the neighbor to lookup information.
        var hashName = getHash(Constants.nodePrefix + i, Constants.totalSpace)
        println("Initializing the peer " + i + " with hash Id " + hashName)
        var node = system.actorOf(Props(new Peer(hashName, Constants.nodePrefix + i, numRequests)), hashName.toString())
        node ! new join(firstNode)
        Thread.sleep(1)
      }
    }
    Thread.sleep(100)
    if (true) {
      for (i <- 1 to numNodes) {
        println("===========================================================")
        Thread.sleep(10)
        var hashName = getHash(Constants.nodePrefix + i, Constants.totalSpace)
        var node = system.actorSelection(Constants.namingPrefix + hashName)
        node ! "print"
        //node ! "printTable"
        Thread.sleep(10)
        println("===========================================================")
      }
    }

    while (Chord.nodesJoined != numNodes) {
      Thread.sleep(1000)
      println("Still joining.....")
    }
    var initTime = System.currentTimeMillis();
    for (i <- 1 to numNodes) {
      var hashName = getHash(Constants.nodePrefix + i, Constants.totalSpace)
      var node = system.actorSelection(Constants.namingPrefix + hashName)
      node ! new findFile(numRequests);
    }
    println("Total time to initate " + (System.currentTimeMillis() - initTime))
    var temp = 0;
    initTime = System.currentTimeMillis();
    while (fileFound.size != (numNodes * numRequests)) {
      Thread.sleep(1000)
      println("Still searching.....Found so far : " + fileFound.size)
    }
    println("Total time elapsed:    " + (System.currentTimeMillis() - initTime))
    println("Files found:           " + fileFound.size)
    println("Total number of hops:  " + TotalNumofHops)
    println("List of files:       \n" + fileFound)
    System.exit(0)
  }
}

sealed trait Seal
case class join(myRandNeigh: Int) extends Seal
case class hasFileKey(fileKey: String) extends Seal
case class getPredec() extends Seal
case class getSucc(fromNewBie: Boolean) extends Seal
case class setPredec(name: Int, fromNewbie: Boolean) extends Seal
case class setSucc(name: Int, fromNewbie: Boolean) extends Seal
case class findPredecessor(key: Int, origin: Int, reqType: String, data: String) extends Seal
case class findSuccessor(key: Int, origin: Int, Type: String, i: Int) extends Seal
case class Finger(hashKey: Int, node: Int, Type: String, i: Int)
case class updateOthers() extends Seal
case class updatePredecessorTables() extends Seal
case class findFile(numRequests: Int) extends Seal
case class findSingleFile(fileHashName: Int) extends Seal
case class handleDelete(revampAfter: Int) extends Seal

class Peer(val hashName: Int, val abstractName: String, val requests: Int) extends Actor {

  val log = Logging(context.system, this)
  var finger = new HashMap[Int, Int]
  var successor: Int = -1;
  var predecessor: Int = -1;
  var isJoined: Boolean = false;
  var timeStamp = System.currentTimeMillis()
  var searchForPredecs = new ArrayBuffer[Int]

  def getFingerId(index: Int): Int = {
    // Converts 0,1,2 to actual Id hashname + 2^i
    (hashName + Math.pow(2, index).toInt) % Constants.totalSpace
  }

  var noOfFilesToBeFound = -1

  def receive = {

    case join: join => {

      if (join.myRandNeigh == -1) {
        // Implies first Node.
        successor = hashName;
        predecessor = hashName;
        fillFingerTable(-1);
        isJoined = true
        Chord.nodesJoined = Chord.nodesJoined + 1;
        //self ! "printTable"
        log.info("Timetaken to boot node " + hashName + " is " + (System.currentTimeMillis() - timeStamp))
      } else {
        var act = context.actorSelection(Constants.namingPrefix + join.myRandNeigh)
        act ! findPredecessor(hashName, hashName, Constants.setRequest, null)
      }
    }

    case fg: Finger => {
      log.debug("\tType:Finger\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + fg)
      if (fg.Type.equals(Constants.fingerRequest)) {
        if (fg.i <= Constants.m) {
          var curIndex = fg.i + 1
          finger(fg.i) = fg.node
          var nextStart: Int = getFingerId(curIndex)
          if (curIndex < Constants.m) {
            var sucActor = context.actorSelection(Constants.namingPrefix + successor)
            sucActor ! findSuccessor(nextStart, hashName, Constants.fingerRequest, curIndex)
          } else {
            //All neighbors are found. Nothing to do.
            log.debug("All actors are found!!")
            self ! updateOthers()
          }
        } else {
          //All neighbors are found. Nothing to do.
        }
      }
    }

    case uo: updateOthers => {
      var preActor = context.actorSelection(Constants.namingPrefix + predecessor)
      var updateableNodes = new HashMap[Int, Int]
      for (i <- 0 until Constants.m) {
        var temp = -1
        var pow = Math.pow(2, i).toInt;
        if (pow < hashName) {
          temp = (hashName - pow) % Constants.totalSpace
        } else {
          temp = Constants.totalSpace - Math.abs(hashName - pow)
        }
        updateableNodes.put(i, temp);
      }
      log.debug("List of predecs to update : " + updateableNodes)
      for (i <- 0 until Constants.m) {
        var t = updateableNodes(i)
        if (!amIThePredecessor(t)) {
          preActor ! findPredecessor(updateableNodes(i), hashName, Constants.fingerUpdate, "" + i)
        }
      }
      log.info(hashName + "joined with succ : " + successor + " and predec: " + predecessor)
      isJoined = true;
      Chord.nodesJoined = Chord.nodesJoined + 1;
    }

    case gs: getSucc => {
      log.debug("\tType:getSucc\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + gs)
      if (gs.fromNewBie) {
        sender ! setSucc(successor, false)
      }
    }

    case p: getPredec => {

    }

    case ss: setSucc => {
      log.debug("\tType:setSucc\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + ss)
      if (ss.fromNewbie && isJoined) {
        var myOldSuccesor = context.actorSelection(Constants.namingPrefix + successor)
        successor = ss.name
        log.debug("[Update] New successor " + successor)
        myOldSuccesor ! setPredec(ss.name, isJoined)
      } else if (!ss.fromNewbie && !isJoined) {
        successor = ss.name
        log.debug("[Update] New successor " + successor)
        //Since I am a new node, Update the successor info of my new predecessor
        var myNewPredecessor = context.actorSelection(Constants.namingPrefix + predecessor)
        myNewPredecessor ! setSucc(hashName, !isJoined)
        var f = Future {
          Thread.sleep(10)
        }
        f.onComplete {
          case x =>
            log.debug("Finger Initing me : " + hashName + " pre: " + predecessor + " Succ: " + successor)
            finger(0) = successor
            self.tell(Finger(-1, finger(0), Constants.fingerRequest, 0), self)
        }
      }
    }

    case p: setPredec => {
      log.debug("\tType:setPredec\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + p)
      if (isJoined && p.fromNewbie) {
        predecessor = p.name;
        log.debug("[Update] New predecessor " + predecessor)
      } else if (!isJoined && !p.fromNewbie) {
        predecessor = p.name
        log.debug("[Update] New predecessor " + predecessor)
        var act = context.actorSelection(Constants.namingPrefix + predecessor)
        act ! getSucc(!isJoined)
      }
    }

    case f: findPredecessor => {
      /**
       * findPredecessor can be called during boot time. When a key is received whose predecessor
       * is to be found, search if you are the predecessor or else delegate the query to a node
       * which is the closest preceding neighbor to the key.
       *
       * If you are the predecessor to the key, send the message to the actor, whose ref is found in origin.
       *
       */
      log.debug("\tType:findPredecessor\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + f)
      var start = hashName
      var end = successor
      var con1 = (start == end)
      var con2 = (end > start && (f.key >= start && f.key < end))
      var con3 = (end < start && ((f.key >= start && f.key < Constants.totalSpace) || (f.key >= 0 && f.key < end)))

      if (f.reqType.equals(Constants.setRequest)) {
        if (con1 || con2 || con3) {
          log.debug("con1" + con1 + "con2" + con2 + "con3" + con3)
          var act = context.actorSelection(Constants.namingPrefix + f.origin)
          act ! setPredec(start, false)
        } else {
          var closetNeigh = closestPrecedingFinger(f.key);
          if (finger(closetNeigh) == hashName) {
            var act = context.actorSelection(Constants.namingPrefix + f.origin)
            act ! setPredec(start, false)
          } else {
            var act = context.actorSelection(Constants.namingPrefix + finger(closetNeigh))
            act ! f
          }
        }
      } else if (f.reqType.equals(Constants.fingerUpdate)) {
        log.debug("convalue : " + con1 + con2 + con3)
        if (con1 || con2 || con3) {
          var c1 = (finger(f.data.toInt) > f.origin)
          var c2 = ((finger(f.data.toInt) < f.origin) && (finger(f.data.toInt) <= hashName))
          log.debug("Found predec for :" + f.key + " " + finger(f.data.toInt) + " " + c1 + c2)
          if (c1 || c2) {
            log.debug("Updating finger value from " + finger(f.data.toInt) + " to " + f.origin)
            finger(f.data.toInt) = f.origin
          }
        } else {
          // Check if you are the successor. If you are the successor, then your predecessor is the actual predecessor for this key.
          var act = null
          if (amITheSuccessor(f.key)) {
            //Route the query to your predecessor, it will find itself.
            var act = context.actorSelection(Constants.namingPrefix + predecessor)
            act ! f
          } else {
            var closetNeigh = closestPrecedingFinger(f.key);
            var act = context.actorSelection(Constants.namingPrefix + finger(closetNeigh))
            act ! f
          }
        }
      }
    }

    case s: findSuccessor => {
      if (s.Type.equals(Constants.fingerRequest)) {
        log.debug("\tType:findSuccessor\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + s)
        var start = hashName
        var end = successor

        // If my successor is the successor I am looking for.
        var con1 = (start == end)
        var con2 = (end > start && (s.key >= start && s.key < end))
        var con3 = (end < start && ((s.key >= start && s.key < Constants.totalSpace) || (s.key >= 0 && s.key < end)))

        // If I am the successor I am looking for
        var con4 = false
        if (predecessor < hashName)
          con4 = (predecessor < s.key && s.key < hashName)
        else {
          var c1 = (predecessor < s.key && s.key < Constants.totalSpace)
          var c2 = (0 < s.key && s.key < hashName)
          con4 = (c1 && !c2) || (!c1 && c2)
        }
        log.debug("convalue : " + con1 + con2 + con3 + con4)

        if (con1 || con2 || con3) {
          // My successor is the succesor of the given key 
          var act = context.actorSelection(Constants.namingPrefix + s.origin)
          act ! Finger(s.key, end, s.Type, s.i)
        } else if (con4) {
          // I am the successor
          var act = context.actorSelection(Constants.namingPrefix + s.origin)
          act ! Finger(s.key, hashName, s.Type, s.i)
        } else {
          var closetNeigh = closestPrecedingFinger(s.key);
          log.debug("[findSuccessor] Key : " + s.key + " Neigh : " + closetNeigh)
          if (finger(closetNeigh) == hashName) {
            var act = context.actorSelection(Constants.namingPrefix + s.origin)
            act ! Finger(s.key, finger(closetNeigh), s.Type, s.i)
          } else {
            var act = context.actorSelection(Constants.namingPrefix + finger(closetNeigh))
            act ! findSuccessor(s.key, s.origin, s.Type, s.i)
          }
        }
      }
    }

    case fr: findFile => {

      var timestamp = System.currentTimeMillis()
      var reqsTobeMade = fr.numRequests
      val scheduler = context.system.scheduler

      for (i <- 1 to fr.numRequests) {
        var fileStringName = Constants.namingPrefix + hashName + i;
        var fileHashName = Chord.getHash(fileStringName, Constants.totalSpace)
        scheduler.scheduleOnce(Duration(i, TimeUnit.SECONDS), self, findSingleFile(fileHashName))
      }
      log.info("created in " + (System.currentTimeMillis() - timestamp))
    }

    case fsf: findSingleFile => {
      log.debug("search running" + System.currentTimeMillis())
      Chord.TotalNumofHops = Chord.TotalNumofHops + 1;
      var fileHashName = fsf.fileHashName
      var found: Boolean = false;
      var maxPredecessor: Int = -1;

      var isForwarded: Boolean = false
      if (amITheSuccessor(fileHashName)) {
        foundFile(fileHashName, hashName)
        found = true;
      }

      if (!found && amIThePredecessor(fileHashName)) {
        var act = context.actorSelection(Constants.namingPrefix + successor)
        act ! findSingleFile(fileHashName)
        isForwarded = true
      }

      if (!found && !isForwarded) {
        var act = context.actorSelection(Constants.namingPrefix + finger(closestPrecedingFinger(fileHashName)))
        act ! findSingleFile(fileHashName)
      }
    }

    case x: String => {
      if (x.equals("print")) {
        log.info("My name " + hashName + " Pre: " + predecessor + " Succ: " + successor)
      } else if (x.equals("printTable")) {
        printTable()
      }
    }

  }

  def foundFile(fileName: Int, foundAt: Int): Unit = {
    Chord.fileFound(fileName) = foundAt
  }

  def printTable(): Unit = {
    var x = " "
    for (i <- 0 to finger.size - 1) {
      x = x + i + "-" + getFingerId(i) + "-" + finger(i) + " , "
    }
    log.debug("My name " + hashName + " Fingers: Size : " + finger.size + x)
  }

  def fillFingerTable(myRandNeigh: Int): Unit = {
    if (myRandNeigh == -1) {
      for (i <- 0 until Constants.m) {
        finger(i) = hashName
      }
    }
  }

  def amITheSuccessor(key: Int): Boolean = {

    var amI = false
    if (predecessor < hashName) {
      amI = (predecessor <= key) && (key < hashName)
    } else {
      var con1 = (predecessor <= key) && (key < Constants.totalSpace)
      var con2 = (0 < key) && (key < hashName)
      amI = (con1 && !con2) || (!con1 && con2)
    }
    log.debug("successor : " + successor + " predec : " + predecessor + " amI Successor : " + amI + " Key : " + key)
    amI
  }

  def amIThePredecessor(key: Int): Boolean = {

    var amI = false
    if (successor > hashName) {
      amI = (key <= successor) && (key > hashName)
    } else {
      var con1 = (hashName < key) && (key < Constants.totalSpace)
      var con2 = (0 < key) && (key < successor)
      amI = (con1 && !con2) || (!con1 && con2)
    }
    log.debug("successor : " + successor + " predec : " + predecessor + " amI Predec : " + amI + " Key : " + key)
    amI
  }

  def closestPrecedingFinger(key: Int): Int = {
    var keyFound = Integer.MIN_VALUE
    var farthestNeigh = Integer.MIN_VALUE
    var current = Integer.MIN_VALUE;

    var negativeNeigh = Integer.MAX_VALUE
    var positiveNeigh = Integer.MAX_VALUE
    var positiveValFound = false

    for (i <- 0 until finger.size) {

      var diff = key - finger(i)
      if (0 < diff && diff < positiveNeigh) {
        keyFound = i;
        positiveNeigh = diff
        positiveValFound = true
      } else if (diff < 0 && diff < negativeNeigh && !positiveValFound) {
        keyFound = i;
        negativeNeigh = diff
      }
    }

    log.debug("Neigh found : " + finger(keyFound) + " for key " + key)
    printTable()
    keyFound
  }
}
