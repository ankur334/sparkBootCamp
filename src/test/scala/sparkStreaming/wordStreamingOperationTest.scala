package sparkStreaming

import org.scalatest.funsuite.AnyFunSuite
import sparkStreaming.sources.FileSources

class wordStreamingOperationTest  extends AnyFunSuite{

  test("Test word count Analytics") {
//    WordCountStreaming.main()
    FileSources.main(Array())
  }

  test("Test word start with Analytics") {
    WordSearching.main(Array("30"))
  }


}
