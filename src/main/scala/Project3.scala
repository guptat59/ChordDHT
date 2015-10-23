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
        println("Initializing the first peer with hash Id " + firstNode)
        node_1 = system.actorOf(Props(new Peer(firstNode, Constants.nodePrefix + i)), firstNode.toString())
        node_1 ! new join(-1)
        Thread.sleep(1000)
        println("First Node" + firstNode)
      } else {
        Thread.sleep(5000)
        // Use the firstNode as the neighbor to lookup information.
        var hashName = getHash(Constants.nodePrefix + i, Constants.totalSpace)
        println("Initializing the peer " + i + " with hash Id " + hashName)
        var node = system.actorOf(Props(new Peer(hashName, Constants.nodePrefix + i)), hashName.toString())
        node ! new join(firstNode)
        Thread.sleep(5000)
      }
    }
    Thread.sleep(1000)
    for (i <- 1 to numNodes) {
      println("===========================================================")
      Thread.sleep(1000)
      var hashName = getHash(Constants.nodePrefix + i, Constants.totalSpace)
      var node = system.actorSelection(Constants.namingPrefix + hashName)
      node ! "print"
      node ! "printTable"
      Thread.sleep(1000)
      println("===========================================================")
    }
    //System.exit(0)
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
case class findPredecessor(key: Int, origin: Int, reqType: String, data: String) extends Seal
case class findSuccessor(key: Int, origin: Int, Type: String, i: Int) extends Seal
case class Finger(hashKey: Int, node: Int, Type: String, i: Int)
case class updateOthers() extends Seal
case class updatePredecessorTables() extends Seal

class Peer(val hashName: Int, val abstractName: String) extends Actor {

  object Consts {
    val fingerRequest = "fingerRequest"
    val setRequest = "setRequest"
    val fingerUpdate = "fingerUpdate"
  }

  val log = Logging(context.system, this)
  // var range1: Range = null;
  // var range2: Range = null;
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

  def receive = {
    case join: join => {

      if (join.myRandNeigh == -1) {
        // Implies first Node.
        //range1 = hashName to Project2.totalSpace - 1
        //range2 = 0 to hashName-1
        successor = hashName;
        predecessor = hashName;
        fillFingerTable(-1);
        isJoined = true
        self ! "printTable"
        println("Timetaken to boot node " + hashName + " is " + (System.currentTimeMillis() - timeStamp))
      } else {
        var act = context.actorSelection(Constants.namingPrefix + join.myRandNeigh)
        act ! findPredecessor(hashName, hashName, Consts.setRequest, null)
      }
    }

    case ft: initFingerTable => {
      //var prevStart: Int = (hashName + Math.pow(2, 0).toInt) % Constants.totalSpace
      var prevStart: Int = 0
      finger(prevStart) = successor
      var loopOnce: Boolean = false
      for (i <- 1 to Constants.m - 1) {
        //var start: Int = (hashName + Math.pow(2, i).toInt) % Constants.totalSpace
        var start: Int = getFingerId(i);
        finger(i) = -1
        if (getFingerId(i) < finger(prevStart)) {
          finger(i) = finger(prevStart)
        } else if (!loopOnce) {
          loopOnce = !loopOnce
          var sucActor = context.actorSelection(Constants.namingPrefix + successor)
          sucActor ! findSuccessor(start, hashName, Consts.fingerRequest, i)
        }
        prevStart = i
      }
      //println(finger)
    }

    case fg: Finger => {
      log.debug("\tType:Finger\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + fg)
      if (fg.Type.equals(Consts.fingerRequest)) {
        if (fg.i <= Constants.m) {
          var curIndex = fg.i + 1
          //finger(a.hashKey) = a.node
          finger(fg.i) = fg.node
          var nextStart: Int = (hashName + Math.pow(2, curIndex).toInt) % Constants.totalSpace
          while (nextStart < fg.node && curIndex < Constants.m) {
            //finger(nextStart) = a.node
            finger(curIndex) = fg.node
            curIndex = curIndex + 1
            log.debug("nextStart : " + nextStart + " a.node : " + fg.node + " i : " + curIndex)
            nextStart = (hashName + Math.pow(2, curIndex).toInt) % Constants.totalSpace
          }
          if (curIndex < Constants.m) {
            var sucActor = context.actorSelection(Constants.namingPrefix + successor)
            sucActor ! findSuccessor(nextStart, hashName, Consts.fingerRequest, curIndex)
          } else {
            //All neighbors are found. Nothing to do.
            println("All actors are found!!")
            self ! "printTable"
            self ! updateOthers()
          }
        } else {
          //All neighbors are found. Nothing to do.
          println("All actors are found")
          self ! "printTable"
          //self ! updateOthers()
        }
      }
    }

    case uo: updateOthers => {
      for (i <- 0 until Constants.m) {
        //var i = 0
        var temp = -1
        var pow = Math.pow(2, i).toInt;
        if (pow < hashName) {
          temp = (hashName - pow) % Constants.totalSpace
        } else {
          temp = Constants.totalSpace - Math.abs(hashName - pow)
        }
        self ! findPredecessor(temp, hashName, Consts.fingerUpdate, "" + i)
        //searchForPredecs.append(temp+"#"+i)
      }
      //isJoined = true;
      // log.debug("Predecs " + searchForPredecs)
      // self ! updatePredecessorTables()
    }

    case upt: updatePredecessorTables => {
      if (searchForPredecs.length > 0) {
        var temp = searchForPredecs(0)
        searchForPredecs.remove(0);
      }
    }

    case h: hasFileKey => {

      var found = false;

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
            println("Finger Initing me : " + hashName + " pre: " + predecessor + " Succ: " + successor)
            self.tell(initFingerTable(), self)
        }
        //f.onComplete { case x => self.tell("print", self) }
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

      if (f.reqType.equals(Consts.setRequest)) {
        if (con1 || con2 || con3) {
          var act = context.actorSelection(Constants.namingPrefix + f.origin)
          act ! setPredec(start, false)
        } else {
          var closetNeigh = closestPrecedingFinger(f.key);
          var act = context.actorSelection(Constants.namingPrefix + finger(closetNeigh))
          act ! f
        }
      } else if (f.reqType.equals(Consts.fingerUpdate)) {
        if (con1 || con2 || con3) {

          var c1 = (finger(f.data.toInt) > f.origin)
          var c2 = ((finger(f.data.toInt) < f.origin) && (finger(f.data.toInt) <= hashName))

          if (c1 || c2) {
            finger(f.data.toInt) = f.origin
            var act = context.actorSelection(Constants.namingPrefix + predecessor)
            act ! f
          }
          /*// Update the finger table.          
          var condition1 = (hashName to finger(f.data.toInt) contains f.origin)
          log.debug(hashName + "||" + f.data.toInt + "||" + finger(f.data.toInt) + "||" + f.origin + "||" + condition1)
          if (finger(f.data.toInt) < hashName) {
            condition1 = ((hashName to Constants.totalSpace contains f.origin) && !(0 to finger(f.data.toInt) contains f.origin)) ||
              (!((hashName to Constants.totalSpace contains f.origin) && (0 to finger(f.data.toInt) contains f.origin)))
          }
          log.debug(hashName + "||" + f.data.toInt + "||" + finger(f.data.toInt) + "||" + f.origin + "||" + condition1 + "||" + con1)
          if (condition1 || con1) {
            log.debug("Found predec : " + hashName + " of key " + f.key + " and updating f(i) " + f.data.toInt)
            finger(f.data.toInt) = f.origin
            var act = context.actorSelection(Constants.namingPrefix + predecessor)
            act ! f
          } // Else Do nothing.            
*/ } else {
          var closetNeigh = closestPrecedingFinger(f.key);
          var act = context.actorSelection(Constants.namingPrefix + finger(closetNeigh))
          act ! f
        }
      }

    }

    case s: findSuccessor => {
      /**
       *
       */
      if (s.Type.equals(Consts.fingerRequest)) {
        log.debug("\tType:findSuccessor\t\tFrom:" + sender.path + "\tTo:" + hashName + "\t Msg: " + s)
        var start = hashName
        var end = successor
        if (s.key >= start && s.key < end) {
          var act = context.actorSelection(Constants.namingPrefix + s.origin)
          act ! Finger(s.key, end, s.Type, s.i)
        } else {
          var closetNeigh = closestPrecedingFinger(s.key);
          try {
            log.debug("[findSuccessor] Key : " + s.key + " Neigh : " + closetNeigh)
          } catch {
            case t: NoSuchElementException =>
              log.debug("Failed to find key for " + s.key + " as the neigh is " + closetNeigh)
              t.printStackTrace()
          }

          if (finger(closetNeigh) == hashName) {
            var act = context.actorSelection(Constants.namingPrefix + s.origin)
            act ! Finger(s.key, finger(closetNeigh), s.Type, s.i)
          } else {
            var act = context.actorSelection(Constants.namingPrefix + finger(closetNeigh))
            println(finger)
            act ! findSuccessor(s.key, s.origin, s.Type, s.i)
          }

        }
      }
    }

    case x: String => {
      if (x.equals("print")) {
        log.debug("My name " + hashName + " Pre: " + predecessor + " Succ: " + successor)
      } else if (x.equals("printTable")) {
        var x = " "
        for (i <- 0 to finger.size - 1) {
          x = x + i + "-" + getFingerId(i) + "-" + finger(i) + " , "
        }
        log.debug("My name " + hashName + " Fingers: Size : " + finger.size + x)
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
        finger(i) = hashName
        //finger((hashName + Math.pow(2, i).toInt) % Constants.totalSpace) = hashName
      }
      //println("fillFingerTable" + finger)
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
    var farthestNeigh = Integer.MIN_VALUE
    var current = Integer.MIN_VALUE;

    //log.debug("Map size : " + finger.size + finger.toString())

    for (i <- 0 until Constants.m) {
      current = finger(i)
      if (farthestNeigh < current) farthestNeigh = i
      if (finger(i) < key) {
        // Do nothing, continue loop search
      } else {
        if (i == 0) {
          keyFound = Constants.m - 1
        } else {
          keyFound = i - 1
        }
        keyFound
      }
    }

    if (keyFound == Integer.MIN_VALUE) {
      //      log.error("This should not happen ####################################")
      //      log.error(finger.toString())
      //      log.error("This should not happen ####################################")
      keyFound = Constants.m - 1
    }
    keyFound
  }

  def closestPrecedingFingerOld(key: Int): Int = {
    var keyFound = Integer.MIN_VALUE
    var farthestNeigh = Integer.MIN_VALUE
    var it = finger.keys.iterator
    while (it.hasNext) {

      var current = it.next().toInt
      if (farthestNeigh < current) farthestNeigh = current
      if (current < key) {
        if (keyFound < current) {
          keyFound = current
        }
      }
    }
    // No assignment happened. Assign the maximum value. 
    if (keyFound == Integer.MIN_VALUE) {
      keyFound = farthestNeigh
    }
    keyFound
  }
}