//#full-example
package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.example.Master.students
import com.example.PasswordWorker.Start
import scala.io.BufferedSource
import scala.util.control.Breaks.{break, breakable}

//#greeter-companion
//#greeter-messages
object PasswordWorker {
  //#greeter-messages
  final val props: Props = Props(new PasswordWorker())
  //#greeter-messages
  final case class Start(data: BufferedSource, i: Int, j: Int)
  //###case object Greet
}
//#greeter-messages
//#greeter-companion

//#greeter-actor
class PasswordWorker() extends Actor {
  import PasswordWorker._
  import Printer._

  var data : BufferedSource = null

  def crackPasswordsInRange(i: Int, j: Int) = {
    breakable {
      for (line <- students.getLines) {
        if (line == "") break
        val cols = line.split(";").map(_.trim)
        // do whatever you want with the columns here
        println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      }
    }
  }


  def receive = {
    case Start(data,i,j) =>
      this.data = data
      this.crackPasswordsInRange(i,j)
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

  def receive = {
    case Greeting(greeting) =>
      log.info("Greeting received (from " + sender() + "): " + greeting)
  }
}

//#printer-actor
object Slave {

}
class Slave extends Actor {
  import scala.util.control.Breaks._
  val system: ActorSystem = ActorSystem("SlaveSystem")

  //#create-actors
  // Create the printer actor
  val passwordworker: ActorRef = system.actorOf(PasswordWorker.props, "PasswordCrackerWorker")
  passwordworker ! Start(students,0,42)
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
