package region.split

import scala.io.Source
import scala.math._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.MD5Hash

class RowkeySplit {

  /**
    * hbase region预分区工具
    *
    * @param filePath    样本文件路径
    * @param numOfSPlits 预分区个数
    **/
  def rowkeySplitedArr(filePath: String, numOfSPlits: Int) = {
    val file = Source.fromFile(filePath).getLines()
    val res = file.map {
      line =>
        MD5Hash.getMD5AsHex(Bytes.toBytes(line))
    }.toList.sorted
    val count = res.length / numOfSPlits
    var str = ""
    for (i <- 0 until numOfSPlits) {
      str += s"\'${res(i * count)}\',"
    }
    println(str)

  }

}
