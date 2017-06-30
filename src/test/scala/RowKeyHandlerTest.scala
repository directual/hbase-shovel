import org.scalatest._

class RowKeyHandlerTest extends FlatSpec with Matchers {

  "transform" should "convert 1_2_x to 2_100_x" in {
    val handler = new RowKeyHandler(Map(1l -> 2l), Map(2l -> 100l))
    handler.transform("1_2_x") should be ("2_100_x")
  }

  it should "convert 999999999999999999999_22_x to 999999999999999999999_22_x" in {
    val handler = new RowKeyHandler(Map(), Map(22l -> 3l))
    handler.transform("999999999999999999999_22_x") should be ("999999999999999999999_22_x")
  }

  it should "convert 1a_2_x to 1a_2_x" in {
    val handler = new RowKeyHandler(Map(1l -> 2l), Map(2l -> 3l))
    handler.transform("1a_2_x") should be ("1a_2_x")
  }

  it should "convert 1_2x to 1_2x" in {
    val handler = new RowKeyHandler(Map(1l -> 2l), Map(2l -> 3l))
    handler.transform("1_2x") should be ("1_2x")
  }
}
