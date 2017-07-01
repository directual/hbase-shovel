
object Main extends App {

  case class Config(sourceTable: String = "",
                    targetTable: String = "",
                    zookeeperQuorum: Option[String] = None,
                    zookeeperZnodeParent: Option[String] = None,
                    networkId: Option[Long] = None,
                    networkIdsMap: Map[Long, Long] = Map(),
                    structIdsMap: Map[Long, Long] = Map())

  val parser = new scopt.OptionParser[Config]("hbase-shovel") {
    head("hbase-shovel")

    opt[String]('i', "input")
      .valueName("<sourceTable>")
      .action((value, config) => config.copy(sourceTable = value))
      .required()
    opt[String]('o', "output")
      .valueName("<targetTable>")
      .action((value, config) => config.copy(targetTable = value))
      .required()
    opt[String]('z', "zookeeperQuorum")
      .valueName("<hbase.zookeeper.quorum>")
      .action((value, config) => config.copy(zookeeperQuorum = Some(value)))
    opt[String]('p', "zookeeperZnodeParent")
      .valueName("<zookeeper.znode.parent>")
      .action((value, config) => config.copy(zookeeperZnodeParent = Some(value)))
    opt[Long]('f', "networkID")
      .valueName("<networkID>")
      .action((x, config) => config.copy(networkId = Some(x)))
      .validate(value =>
        if (value >= 0) success
        else failure("Value <networkID> must be non-negative")
      )
      .text("filter networkId")
    opt[Map[Long, Long]]('n', "networkIdsMap")
      .valueName("<id1=id2,id3=id4...>")
      .action((value, config) => config.copy(networkIdsMap = value))
    opt[Map[Long, Long]]('s', "structIdsMap")
      .valueName("<id1=id2,id3=id4...>")
      .action((value, config) => config.copy(structIdsMap = value))
  }

  parser.parse(args, Config()) match {
    case Some(config) =>
      val shovel = Shovel(config)
      shovel.run()
    case None =>
      sys.exit(1)
  }

}
