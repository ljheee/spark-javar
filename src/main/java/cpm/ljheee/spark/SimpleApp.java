package cpm.ljheee.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 * 请注意，在Spark 2.0之前，Spark的主要编程接口是弹性分布式数据集（RDD）。
 * Spark 2.0之后，RDD被数据集取代，数据集类似于RDD一样强类型，但在底层有更丰富的优化。
 * 仍然支持RDD接口，您可以在RDD编程指南中获得更完整的参考。但是，我们强烈建议您切换到使用Dataset，它具有比RDD更好的性能。
 * 请参阅SQL编程指南以获取有关数据集的更多信息。
 * p15,p39

 */
public class SimpleApp {

    /* JavaRDD => JavaPairRDD: 通过mapToPair函数
        JavaPairRDD => JavaRDD: 通过map函数转换*/


    public static void main(String[] args) throws InterruptedException {

        CountDownLatch cdl = new CountDownLatch(1);

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ljh");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);//无论什么语言客户端，都需要SparkContext

        JavaRDD<String> input = sc.textFile("README.md");

        // 切分为单词
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) {
                        return new HashSet<String>(Arrays.asList(x.split(" "))).iterator();
                    }});

        // 转换为键值对并计数
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>(){
                    public Tuple2<String, Integer> call(String x){
                        return new Tuple2(x, 1);
                    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer x, Integer y){ return x + y;}});

        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> x) throws Exception {
                System.out.println(x);
            }
        });
        cdl.await();

        // 将统计出来的单词总数存入一个文本文件，引发求值 counts.saveAsTextFile(outputFile);


//        String logFile = "README.md"; // Should be some file on your system
//        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
//        Dataset<String> logData = spark.read().textFile(logFile).cache();
//
//        long numAs = logData.filter(s -> s.contains("a")).count();
//        long numBs = logData.filter(s -> s.contains("b")).count();
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//
//        spark.stop();
    }
}
