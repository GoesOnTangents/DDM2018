package com.example

import java.io.File

import com.example._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.example.MasterActor.Read
import com.example.SlaveActor.Subscribe
import com.typesafe.config.ConfigFactory

object Tetropus extends App {
  if (!(args.length == 5 || args.length == 7)) {
    wrong_input
  }

  if (args(0) == "master") {
    invoke_master
  } else if (args(0) == "slave") {
    invoke_slave
  } else wrong_input

  def invoke_master: Unit = {
    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("MasterSystem")

    val system: ActorSystem = ActorSystem("MasterSystem", config)

    val masterActor: ActorRef = system.actorOf(MasterActor.props, "MasterActor")
    masterActor ! Read(args(6))
  }

  def invoke_slave: Unit ={
    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("SlaveSystem")
    val system: ActorSystem = ActorSystem("SlaveSystem", config)
    val slaveActor: ActorRef = system.actorOf(SlaveActor.props, "SlaveActor")
    val masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
    slaveActor ! Subscribe(masterActorAddress)
  }

  def print_usage: Unit = {
    println("Usage: [master|slave] --workers [num] [--slaves <num> |] [--input <file.csv> | --host <ip>]")
  }
  def wrong_input: Unit = {
    println("Wrong input parameters.")
    print_usage
  }
}