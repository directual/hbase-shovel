package com.directual.hbase.shovel

import ActionType._

object Main extends App {

  case class Config(sourceTable: String = "",
                    targetTable: Option[String] = None,
                    zookeeperQuorum: Option[String] = None,
                    zookeeperZnodeParent: Option[String] = None,
                    networkId: Option[Long] = None,
                    networkIdsMap: Map[Long, Long] = Map(),
                    structIdsMap: Map[Long, Long] = Map(),
                    action: ActionType = Copy)

  val parser = new scopt.OptionParser[Config]("hbase-shovel") {
    head("hbase-shovel")

    opt[String]('i', "input")
      .valueName("<sourceTable>")
      .action((value, config) => config.copy(sourceTable = value))
      .required()
    opt[String]('o', "output")
      .valueName("<targetTable>")
      .action((value, config) => config.copy(targetTable = Some(value)))
    opt[String]('a', "action")
      .valueName("<action>")
      .validate(value =>
        if (value == "copy" || value == "remove") success
        else failure("Value <action> can can be either \"copy\", either \"remove\"")
      )
      .action((value, config) =>
        value match {
          case "copy" =>
            config.copy(action = Copy)
          case "remove" =>
            config.copy(action = Remove)
        }
      )
    opt[String]('z', "zookeeperQuorum")
      .valueName("<hbase.zookeeper.quorum>")
      .action((value, config) => config.copy(zookeeperQuorum = Some(value)))
    opt[String]('p', "zookeeperZnodeParent")
      .valueName("<zookeeper.znode.parent>")
      .action((value, config) => config.copy(zookeeperZnodeParent = Some(value)))
    opt[Long]('f', "networkID")
      .valueName("<networkID>")
      .validate(value =>
        if (value >= 0) success
        else failure("Value <networkID> must be non-negative")
      )
      .action((x, config) => config.copy(networkId = Some(x)))
      .text("filter networkId")
    opt[Map[Long, Long]]('n', "networkIdsMap")
      .valueName("<id1=id2,id3=id4...>")
      .action((value, config) => config.copy(networkIdsMap = value))
    opt[Map[Long, Long]]('s', "structIdsMap")
      .valueName("<id1=id2,id3=id4...>")
      .action((value, config) => config.copy(structIdsMap = value))

    checkConfig(config =>
      if (config.action == Copy && config.targetTable.isEmpty) {
        failure("targetTable must be specified for copy action")
      } else if (config.action == Remove && config.targetTable.isDefined) {
        println("removing rows from sourceTable, targetTable argument will be ignored")
        success
      } else {
        success
      }
    )
  }

  parser.parse(args, Config()) match {
    case Some(config) =>
      val shovel = Shovel(config)
      shovel.run()
    case None =>
      sys.exit(1)
  }
}
