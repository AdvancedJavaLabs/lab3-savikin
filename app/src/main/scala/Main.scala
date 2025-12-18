import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.hadoop.fs.FileSystem
import java.io.IOException

import scala.jdk.CollectionConverters.*
import scala.annotation.static

import upickle.default.{ReadWriter => RW, macroRW}
import upickle._

case class Transaction(var transaction_id: Long, var product_id: Long, var category: String, var price: Long, var quantity: Long) {}
object Transaction {
  implicit val rw: RW[Transaction] = macroRW
}

case class CategoryData(var category: String, var revenue: Long, var quantity: Long) {}
object CategoryData {
  implicit val rw: RW[CategoryData] = macroRW
}

class Phase1Mapper extends Mapper[Object, Text, Text, Text] {
  private def parse_price(str: String): Long = (str.toDouble * 1000).toLong

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit =
    if (value != null && !value.toString().contains("transaction_id,product_id,category,price,quantity")) then {
      val data: Array[String] = value.toString().split(",");
      val transaction_id: Long = data(0).toLong;
      val product_id: Long = data(1).toLong;
      val category: String = data(2);
      val price: Long = parse_price(data(3))
      val quantity: Long = data(4).toLong;

      context.write(Text(category), Text(upickle.write(Transaction(transaction_id, product_id, category, price, quantity))));
    }
}

class Phase1Combiner extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    var revenue: Long = 0;
    var quantity: Long = 0;
    for valuesrc <- values.asScala do {
      val value = upickle.read[Transaction](valuesrc.toString())
      revenue = revenue + value.quantity * value.price;
      quantity = quantity + value.quantity;
    }

    context.write(key, Text(upickle.write(CategoryData(key.toString(), revenue, quantity))));
  }
}

class Phase1Reducer extends Reducer[Text, Text, LongWritable, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, LongWritable, Text]#Context): Unit = {
    var revenue: Long = 0;
    var quantity: Long = 0;
    for valuesrc <- values.asScala do {
      val value = upickle.read[CategoryData](valuesrc.toString())
      revenue = revenue + value.revenue;
      quantity = quantity + value.quantity;
    }

    context.write(LongWritable(revenue), Text(upickle.write(CategoryData(key.toString(), revenue, quantity))));
  }
}

class App;
object App {
  @static
  def main(args: Array[String]): Unit = {
    val input_dir = Path("/data");
    val output_dir = Path("/results");

    val jobconf = Configuration();
    val fs = FileSystem.get(jobconf);
    if (fs.exists(output_dir)) then {
      fs.delete(output_dir, true);
    }

    val job1 = Job.getInstance(jobconf, "Lab3");
    job1.setJarByClass(classOf[App]);
    job1.setInputFormatClass(classOf[TextInputFormat]);
    job1.setMapperClass(classOf[Phase1Mapper]);
    job1.setMapOutputKeyClass(classOf[Text]);
    job1.setMapOutputValueClass(classOf[Text]);
    job1.setCombinerClass(classOf[Phase1Combiner]);
    job1.setReducerClass(classOf[Phase1Reducer]);
    job1.setOutputKeyClass(classOf[LongWritable]);
    job1.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job1, input_dir);
    FileOutputFormat.setOutputPath(job1, output_dir);
    job1.waitForCompletion(true);
  }
}

type Mapper2 = Mapper[Object, Text, LongWritable, Text];
class Phase2Mapper extends Mapper2 {
  override def map(key: Object, value: Text, context: Mapper2#Context): Unit = {
    val strs = value.toString().split("\\s").splitAt(1).toList.map(_.mkString)
    context.write(LongWritable(strs(0).toLong * -1), Text(strs(1)));
  }
}

type Reducer2 = Reducer[LongWritable, Text, Void, Text]
class Phase2Reducer extends Reducer2 {
  override def reduce(key: LongWritable, values: java.lang.Iterable[Text], context: Reducer2#Context): Unit =
    for value <- values.asScala do {
      val obj = upickle.read[CategoryData](value.toString());
      context.write(null, Text(f"${obj.category}%-20s ${obj.revenue}%-20s ${obj.quantity}%-20s"));
    }
}

class App2;
object App2 {
  @static
  def main(args: Array[String]): Unit = {
    val input_dir = Path("/results");
    val output_dir = Path("/results2");

    val jobconf = Configuration();
    val fs = FileSystem.get(jobconf);
    if (fs.exists(output_dir)) then {
      fs.delete(output_dir, true);
    }

    val job = Job.getInstance(jobconf, "Lab3Phase2");
    job.setJarByClass(classOf[App]);
    job.setInputFormatClass(classOf[TextInputFormat]);
    job.setMapperClass(classOf[Phase2Mapper]);
    job.setMapOutputKeyClass(classOf[LongWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setReducerClass(classOf[Phase2Reducer]);
    job.setOutputKeyClass(classOf[Void]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, input_dir);
    FileOutputFormat.setOutputPath(job, output_dir);
    job.waitForCompletion(true);
  }
}
