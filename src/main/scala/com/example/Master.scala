package com.example

import java.io.File

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.example.MasterActor.Read
import com.example.SlaveActor.CrackPasswordsInRange
import com.typesafe.config.ConfigFactory

import scala.util.control.Breaks.{break, breakable}

object MasterActor {
  final val props: Props = Props(new MasterActor())
  case object Read
  case object CrackPasswords
  case object SlaveSubscription
  case object PasswordFound
  case object SolveLinearCombination
  case class PasswordFound(index: Int, password: Int)
}

class MasterActor extends Actor {

  import MasterActor._
  var expectedSlaveAmount: Int = 3
  var slaves: Array[ActorRef] = Array()

  var names: Array[String] = Array()
  var hashes: Array[String] = Array()
  var gene: Array[String] = Array()

  //results
  var cracked_passwords: Array[Int] = Array()
  var lcs_index: Array[Int] = Array()
  var linear_combination: Array[Boolean] = Array()
  var partner_hashes: Array[String] = Array()

  //counter variables
  var num_cracked_passwords = 0

  def read(): Unit = {
    val file_contents =
      scala.io.Source.fromFile("students_short.csv").getLines().drop(1) //TODO dont hardcode filename
    breakable {
      for (line <- file_contents) {
        if (line == "") break
        val cols = line.split(";").map(_.trim)
        this.names = this.names :+ cols(1)
        this.hashes = this.hashes :+ cols(2)
        this.gene = this.gene :+ cols(3)

        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      }
    }
    val num_lines = names.length
    cracked_passwords = Array.ofDim(num_lines)
    linear_combination = Array.ofDim(num_lines)
    lcs_index = Array.ofDim(num_lines)
    partner_hashes = Array.ofDim(num_lines)
  }

  override def receive: Receive = {
    case SlaveSubscription =>
      this.subscribe_slaves()
    case CrackPasswords =>
      this.delegate_password_cracking()
    case PasswordFound(id, pw) =>
      this.store_password(id,pw)
    case Read =>
      this.read()
    case SolveLinearCombination =>
      this.solve_linear_combination()
    case msg: Any => throw new RuntimeException("unknown message type " + msg);

  }

  def delegate_password_cracking(): Unit = {
    println("Delegating Passwords to Crack.")
    val ranges = PasswordWorker.range_split(100000, 999999, slaves.length)
    for (i <- slaves.indices) {
      slaves(i) ! CrackPasswordsInRange(hashes, ranges(i)._1, ranges(i)._2)
    }
  }

  def subscribe_slaves(): Unit = {
    this.slaves = this.slaves :+ this.sender()
    println(s"Current master's slaves:\n ${slaves.deep.mkString("\n")}")
    if (slaves.length == expectedSlaveAmount) {
      self ! CrackPasswords
    }
  }

  def store_password(id: Int, password: Int): Unit = {
    cracked_passwords(id) = password
    num_cracked_passwords += 1

    if (num_cracked_passwords == cracked_passwords.length) {
      self ! SolveLinearCombination
    }
  }

  def solve_linear_combination(): Unit = {

  }

  def findLcsPartners(): Unit = {
    //TODO: Step 1: Give slaves genes
    //Step 2: Make threadsafe Index and start distributing one name per slaves.
  }

  def storePartnerAndReassignName(): Unit = {
    //TODO: 1. Store Partner
    //2. IF work exists: Distribute new name
    //   IF not_yet_finished: Do nothing
    //   ELSE start find_linear_combination()
  }



  def find_prefixed_hashes(): Unit = {
    // Give slaves names
    // Distribute new names when finished...
  }
}

object Master extends App {
    if (args.length == 0) {
      println("dude, you didn't give me any parameters")
    }
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
