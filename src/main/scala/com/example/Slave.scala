//#full-example
package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.example.Master.students
import com.example.PasswordWorker.Start
import com.example.Slave.CrackPasswordsInRange

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

object Slave {
  final case class CrackPasswordsInRange()
}
class Slave extends Actor {
  val system: ActorSystem = ActorSystem("SlaveSystem")
  val passwordworker: ActorRef = system.actorOf(PasswordWorker.props, "PasswordCrackerWorker")

  def receive = {
    case CrackPasswordsInRange(data,i,j) =>
      this.passwordworker ! Start(data,i,j)
  }
}

