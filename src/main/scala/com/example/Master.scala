//#full-example
package com.example

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.example.MasterActor.Read
import com.example.SlaveActor.CrackPasswordsInRange
import com.typesafe.config.ConfigFactory

import scala.io.BufferedSource
import scala.util.control.Breaks.{break, breakable}

object MasterActor {
  final val props: Props = Props(new MasterActor())
  case object Read
  case object CrackPasswords
  case object SlaveSubscription
  final case class PasswordFound(index: Int, password: Int)
  //###case object Greet
}

class MasterActor extends Actor {
  import MasterActor._

  var data : BufferedSource = null
  var expectedSlaveAmount : Int = 2
  var slaves: Array[ActorRef] = Array()

  var names : Array[String] = Array()
  var hashes : Array[String] = Array()
  var gene : Array[String] = Array()

  //results
  var cracked_passwords : Array[Int] = Array() //TODO: Initialize to size of csv
  var lcs_index: Array[Int] = Array()
  var linear_combination: Array[Boolean] = Array()
  var partner_hashes: Array[String] = Array()

  def read(): Unit = {
    this.data = scala.io.Source.fromFile("students.csv") //TODO: don't hardcode this
    breakable {
      for (line <- this.data.getLines.drop(1)) {
        if (line == "") break
        val cols = line.split(";").map(_.trim)
        this.names = this.names :+ cols(1)
        this.hashes = this.hashes :+ cols(2)
        this.gene = this.gene :+ cols(3)

        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      }
    }
  }

  def delegatePasswordCracking(): Unit = {
    println("Delegating Passwords to Crack.")
    var range_per_slave = 1000000/this.slaves.size
    var i = 0
    var j = 0 + range_per_slave
    for (s <- this.slaves) {
      s ! CrackPasswordsInRange(this.hashes, i, j)
      i += range_per_slave
      j += range_per_slave
    }

  }
  def subscribeSlaves(): Unit = {
    this.slaves = this.slaves :+ this.sender()
    if (this.slaves.size == this.expectedSlaveAmount) this.delegatePasswordCracking()
    println(s"Current master's slaves: $this.slaves")
  }
  def storePassword(id: Int, password: Int): Unit = {
    //TODO: Store password at index
    //TODO: IF all passwords => find_lcs_partners()
  }

  def findLcsPartners(): Unit ={
    //TODO: Step 1: Give slaves genes
    //Step 2: Make threadsafe Index and start distributing one name per slaves.
  }

  def storePartnerAndReassignName(): Unit = {
    //TODO: 1. Store Partner
    //2. IF work exists: Distribute new name
    //   IF not_yet_finished: Do nothing
    //   ELSE start find_linear_combination()
  }

  def findLinearCombination(): Unit = {
    /* Sum all passwords
    1.ALEX MACHT HIER MAGIE

    */
  }

  def find_prefixed_hashes(): Unit = {
    // Give slaves names
    // Distribute new names when finished...
  }


  override def receive: Receive = {
    case SlaveSubscription =>
      this.subscribeSlaves()
    case CrackPasswords =>
      this.delegatePasswordCracking()
    case PasswordFound(id, password) =>
      this.storePassword(id, password)
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
  masterActor ! Read


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
