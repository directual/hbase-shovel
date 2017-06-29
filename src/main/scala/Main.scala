import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._

object Main extends App {

  def transformRow(rowId : String) : String = {
    return rowId + "-copy"
  }

  def copyToTable(sourceTable : Table, targetTable : Table, transformRow : String => String) : Unit = {

    val scan = new Scan()
    val scanner = sourceTable.getScanner(scan)
    var result = scanner.next()

    while (result != null) {
      val rowId = new String(result.getRow)
      val put = new Put(transformRow(rowId).getBytes())

      println(rowId)

      for(cell <- result.rawCells()) {
        put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell))
      }
      targetTable.put(put)

      result = scanner.next()
    }

  }

  case class Config(sourceTable : String = "",
                    targetTable : String = "")

  val parser = new scopt.OptionParser[Config]("hbase-shover") {
    head("hbase-shover")

    opt[String]('s', "input")
      .valueName("<sourceTable>")
      .action((value, config) => config.copy(sourceTable = value))
      .required()
    opt[String]('o', "output")
      .valueName("<targetTable>")
      .action((value, config) => config.copy(targetTable = value))
      .required()
  }


  var config = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(config)

//  checkConfig((c : Config) => {
//    val admin = connection.getAdmin
//    if (!admin.tableExists(TableName.valueOf(c.sourceTable))) {
//      println(s"table ${c.sourceTable} doesn't exist")
//    }
//    if (!admin.tableExists(TableName.valueOf(c.targetTable))) {
//      println(s"table ${c.targetTable} doesn't exist")
//    }
//  })

  parser.parse(args, Config()) match {
    case Some(c) =>
      val table1 = connection.getTable(TableName.valueOf(c.sourceTable))
      val table2 = connection.getTable(TableName.valueOf(c.targetTable))
      copyToTable(table1, table2, transformRow)
      sys.exit(0)
    case None =>
      sys.exit(0)
  }

}
