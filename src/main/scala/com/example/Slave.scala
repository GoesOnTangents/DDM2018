//#full-example
package com.example

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, ActorSystem, Props}
import com.example.MasterActor.SlaveSubscription
import com.example.PasswordWorker.Start
import com.example.SlaveActor.{CrackPasswordsInRange, Subscribe}
import com.typesafe.config.ConfigFactory

object PasswordWorker {
  final val props: Props = Props(new PasswordWorker())
  final case class Start(passwords: Array[String], i: Int, j: Int)
}
class PasswordWorker() extends Actor {
  import PasswordWorker._
  import Printer._

  def crackPasswordsInRange(passwords: Array[String], i: Int, j: Int) = {
    //fancy cracking functionality
    //send each password
  }

  override def receive: Receive = {
    case Start(passwords,i,j) =>
      this.crackPasswordsInRange(passwords,i,j)
    }
}

object SlaveActor {
  final case class CrackPasswordsInRange(passwords: Array[String], i: Int, j: Int)
  final val props: Props = Props(new SlaveActor())
  final case class Subscribe(addr: String)
  final case class
}

class SlaveActor extends Actor {
  val system: ActorSystem = ActorSystem("SlaveSystem")
  val passwordworker: ActorRef = system.actorOf(PasswordWorker.props, "PasswordCrackerWorker")

  override def receive: Receive = {
    case CrackPasswordsInRange(passwords,i,j) =>
      this.passwordworker ! Start(passwords,i,j) //TODO: delegate work
    case Subscribe(addr) =>
      this.Subscribe(addr)
  }

  def Subscribe(addr: String) = {
    val selection = context.actorSelection(addr)
    selection ! SlaveSubscription
  }
}

object Slave extends App {
  val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("SlaveSystem")
  val system: ActorSystem = ActorSystem("SlaveSystem", config)
  val slaveActor: ActorRef = system.actorOf(SlaveActor.props, "SlaveActor")
  val addr: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  slaveActor ! Subscribe(addr)

}

