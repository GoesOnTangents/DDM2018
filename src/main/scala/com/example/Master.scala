//#full-example
package com.example

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.example.MasterActor.Read
import com.typesafe.config.ConfigFactory

import scala.io.BufferedSource
import scala.util.control.Breaks.{break, breakable}

object MasterActor {
  final val props: Props = Props(new MasterActor())
  case object Read
  case object CrackPasswords
  final case class CrackPasswordsInRange(bufferedSource: BufferedSource, i: Int, j: Int)
  case object SlaveSubscription

  //###case object Greet
}

class MasterActor extends Actor {
  import MasterActor._

  var data : BufferedSource = null
  var expectedSlaveAmount : Int = 1
  var slaves: Array[ActorRef] = Array()

  def read(): Unit = {
    this.data = scala.io.Source.fromFile("students.csv")
  }

  def delegatePasswordCracking(): Unit = {
    var range = 1000000/this.slaves.size
    var i = 0
    var j = 0 + range
    for (s <- this.slaves) {
      s ! CrackPasswordsInRange(this.data, i, j)
      i += range
      j += range
    }

  }
  def subscribeSlaves(): Unit = {
    this.slaves = this.slaves :+ this.sender()
    if (this.slaves.size == this.expectedSlaveAmount) this.delegatePasswordCracking()
    println(s"Current master's slaves: $this.slaves")
  }

  override def receive: Receive = {
    case SlaveSubscription =>
      this.subscribeSlaves()
    case CrackPasswords =>
      this.delegatePasswordCracking()
    case Read =>
      this.read()

    }
}
//#greeter-actor

//#printer-companion
//#printer-messages
object Printer {
  //#printer-messages
  def props: Props = Props[Printer]
  //#printer-messages
  final case class Greeting(greeting: String)
}
//#printer-messages
//#printer-companion

//#printer-actor
class Printer extends Actor with ActorLogging {
  import Printer._

  def receive: Receive = {
    case Greeting(greeting) =>
      log.info("Greeting received (from " + sender() + "): " + greeting)
  }
}

//#printer-actor

object Master extends App {
  val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("MasterSystem")

  val system: ActorSystem = ActorSystem("MasterSystem", config)

  val masterActor: ActorRef = system.actorOf(MasterActor.props, "MasterActor")
  masterActor ! Read()


}



















//#main-class
/*object AkkaQuickstart extends App {
  import Greeter._

  // Create the 'helloAkka' actor system
  val system: ActorSystem = ActorSystem("helloAkka")

  //#create-actors
  // Create the printer actor
  val printer: ActorRef = system.actorOf(Printer.props, "printerActor")

  // Create the 'greeter' actors
  val howdyGreeter: ActorRef =
    system.actorOf(Greeter.props("Howdy", printer), "howdyGreeter")
  val helloGreeter: ActorRef =
    system.actorOf(Greeter.props("Hello", printer), "helloGreeter")
  val goodDayGreeter: ActorRef =
    system.actorOf(Greeter.props("Good day", printer), "goodDayGreeter")
  //#create-actors

  //#main-send-messages
  howdyGreeter ! WhoToGreet("Akka")
  howdyGreeter ! Greet

  howdyGreeter ! WhoToGreet("Lightbend")
  howdyGreeter ! Greet

  helloGreeter ! WhoToGreet("Scala")
  helloGreeter ! Greet

  goodDayGreeter ! WhoToGreet("Play")
  goodDayGreeter ! Greet
  //#main-send-messages
}
//#main-class
*/
//#full-example
