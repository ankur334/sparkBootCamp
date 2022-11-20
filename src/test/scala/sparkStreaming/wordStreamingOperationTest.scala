package sparkStreaming

import org.scalatest.funsuite.AnyFunSuite

class wordStreamingOperationTest  extends AnyFunSuite{

  test("Test word count Analytics") {
    WordCountStreaming.main()
  }

  test("Test word start with Analytics") {
    WordSearching.main(Array("30"))
  }


}
