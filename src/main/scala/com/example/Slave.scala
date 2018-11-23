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
    var id = 0
    var password = 0
    println(s"$this has started to go through passwords from $i to $j")
    //fancy cracking functionality
    //send each password

    for (x <- i until j){
      println(s"${hash(x.toString())}")
      //here we should actually compare and return
    }
    def hash(s: String): String = {
      val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
      m.map("%02x".format(_)).mkString
    }
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
}

class SlaveActor extends Actor {
  val system: ActorSystem = ActorSystem("SlaveSystem")
  val passwordWorker: ActorRef = system.actorOf(PasswordWorker.props, "PasswordCrackerWorker")

  override def receive: Receive = {
    case CrackPasswordsInRange(passwords,i,j) =>
      this.passwordWorker ! Start(passwords,i,j) //TODO: delegate work
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

