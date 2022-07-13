package region.split

object test {

  def main(args: Array[String]): Unit = {
    val split = new RowkeySplit
    split.rowkeySplitedArr("/Users/jiang.li/IdeaProjects/self/learn/hbase/src/main/resources/union.txt",20)
  }


}
