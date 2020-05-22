package producer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class TransfertToHbase {
    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("read file");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Producer producer = new Producer();

        //leggo file da hdfs
       /* List<String> list1 = sc.textFile("hdfs://localhost:54310/marco/sabd/query1")
                .collect();

        List<String> list2 = sc.textFile("hdfs://localhost:54310/marco/sabd/query2")
                .collect();
        List<String> list3 = sc.textFile("hdfs://localhost:54310/marco/sabd/query3")
                .collect();

        producer.sendToHbase(list1,"hbase");
        producer.sendToHbase(list2,"hbase");
        producer.sendToHbase(list3,"hbase");*/
    }
}
