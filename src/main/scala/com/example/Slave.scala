package com.example

import java.io.File

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.example.MasterActor.{PasswordFound, SlaveSubscription}
import com.example.PasswordWorker.Start
import com.example.SlaveActor.{CrackPasswordsInRange, Subscribe}
import com.typesafe.config.ConfigFactory

object PasswordWorker {
  final val props: Props = Props(new PasswordWorker())
  final case class Start(passwords: Array[String], i: Int, j: Int)

  //all values inclusive
  def range_split(min: Int, max: Int, num_batches: Int): Vector[Tuple2[Int,Int]] = {
    var result = Vector[Tuple2[Int,Int]]()

    val total_range = max - min + 1

    val range_per_batch = total_range / num_batches
    val additional_stuff_for_first_batch = total_range % num_batches
    var i = min
    var j = min + range_per_batch - 1 + additional_stuff_for_first_batch
    for (_ <- 1 to num_batches) {
      result = result :+ (i, j)
      i = j + 1
      j += range_per_batch
    }

    result
  }
}
class PasswordWorker() extends Actor {
  import PasswordWorker._

  def crack_passwords_in_range(passwords: Array[String], i: Int, j: Int) : Unit = {
    val masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
    val masterActor = context.actorSelection(masterActorAddress)
    println(s"$this has started to go through passwords from $i to $j")
    //fancy cracking functionality
    //send each password

    var id  = 0
    for (password <- i until j){
      val hashed_password = hash(password.toString)
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
    context.stop(self)
  }

  def hash(s: String): String = {
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }

  override def receive: Receive = {
    case Start(passwords,i,j) =>
      this.crack_passwords_in_range(passwords,i,j)
    }
}

object SlaveActor {
  final case class CrackPasswordsInRange(passwords: Array[String], i: Int, j: Int)
  final val props: Props = Props(new SlaveActor())
  final case class Subscribe(addr: String)
}

class SlaveActor extends Actor {

  override def receive: Receive = {
    case CrackPasswordsInRange(passwords,i,j) =>
      this.start_password_workers(passwords, i, j)
    case Subscribe(addr) =>
      this.subscribe(addr)
  }

  def subscribe(addr: String) : Unit = {
    val selection = context.actorSelection(addr)
    selection ! SlaveSubscription
  }

  def start_password_workers(passwords: Array[String], min: Int, max: Int): Unit = {
    val num_local_workers = 2 //TODO dont hardcode
    val ranges = PasswordWorker.range_split(min, max, num_local_workers)
    for (i <- 0 until num_local_workers) {
      val worker = context.actorOf(PasswordWorker.props, "PasswordCrackerWorker" + i)
      worker ! Start(passwords, ranges(i)._1,ranges(i)._2)
    }

  }
}

//legacy:
object Slave extends App {
  val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("SlaveSystem")
  val system: ActorSystem = ActorSystem("SlaveSystem", config)
  val slaveActor: ActorRef = system.actorOf(SlaveActor.props, "SlaveActor")
  val masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  slaveActor ! Subscribe(masterActorAddress)


}

