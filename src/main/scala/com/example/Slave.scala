package com.example

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, ActorSelection, ActorSystem, Props}
import com.example.MasterActor.{PasswordFound, SlaveSubscription}
import com.example.PasswordWorker.Start
import com.example.SlaveActor.{CrackPasswordsInRange, Subscribe}
import com.typesafe.config.ConfigFactory

object PasswordWorker {
  final val props: Props = Props(new PasswordWorker())
  final case class Start(passwords: Array[String], i: Int, j: Int)
}
class PasswordWorker() extends Actor {
  import PasswordWorker._

  def crackPasswordsInRange(passwords: Array[String], i: Int, j: Int) = {
    val masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
    val masterActor = context.actorSelection(masterActorAddress)
    println(s"$this has started to go through passwords from $i to $j")
    //fancy cracking functionality
    //send each password

    var id  = 0;
    for (password <- i until j){
      val hashed_password = hash(password.toString);
      //println(s"${hashed_password}")
      id = 0
      for (password_hash <- passwords) {
        if (password_hash == hashed_password) {
          masterActor ! PasswordFound(id, password)
          println("cracked password " + password_hash + ": " + password)
        }
        id += 1
      }
    }
    println("range completed")
  }

  def hash(s: String): String = {
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
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
  val passwordWorker: ActorRef = context.actorOf(PasswordWorker.props, "PasswordCrackerWorker")

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
  val masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  slaveActor ! Subscribe(masterActorAddress)


}

