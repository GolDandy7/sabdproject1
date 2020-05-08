import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import utility.Outlet;
import utility.OutletParser;

import java.time.LocalDate;
import java.util.Date;

public class Query1 {

    private static String pathToFile = "src/dataset/covid19datinazionali.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logLines = sc.textFile(pathToFile);
        String firstRow = logLines.first();
        JavaRDD<String> covid1 = logLines.filter(x -> !x.equals(firstRow));


        JavaRDD<Outlet> outlets =
                covid1.map(
                        line -> OutletParser.parseCSV(line));


        JavaRDD<Tuple3<String, Integer, Integer>> result = outlets.map(x -> new Tuple3<String, Integer, Integer>
                (x.getWeek(), x.getGuariti(), x.getTamponi()));

        /*String secondRow = covid1.first();

        JavaRDD<String> covid2 = covid1.filter(x -> !x.equals(secondRow));
        JavaRDD<Outlet> outlet2 =
                covid2.map(
                        line -> OutletParser.parseCSV(line));
        JavaRDD<Tuple3<String, Integer, Integer>> result2 = outlet2.map(x -> new Tuple3<String, Integer, Integer>
                (x.getWeek(), x.getGuariti(), x.getTamponi()));*/



        /*//per pulire l'RDD dai dati cumulativi
        JavaRDD<Tuple3<LocalDate, Integer, Integer>> result11 = outlets.map(x -> new Tuple3<LocalDate, Integer, Integer>
                (x.getDateTime(), x.getGuariti(), x.getTamponi()));
        JavaRDD<Tuple3<LocalDate, Integer, Integer>> result3 = outlets.map(x -> new Tuple3<LocalDate, Integer, Integer>
                (x.getDateTime().minusDays(1), x.getGuariti(), x.getTamponi()));*/

        JavaPairRDD<LocalDate,Integer> result5= outlets.mapToPair(x -> new Tuple2<>(x.getDateTime(),x.getGuariti()));
        JavaPairRDD<LocalDate,Integer> result4= outlets.mapToPair(x ->new Tuple2<>(x.getDateTime().plusDays(1), x.getGuariti()));
        JavaPairRDD<LocalDate,Integer>result6=result5.union(result4).reduceByKey((a,b) -> Math.abs(b-a)).filter(x-> !x._1().toString().equals("2020-05-07")).sortByKey();
         JavaPairRDD<LocalDate, String> result7= outlets.mapToPair(x -> new Tuple2<>(x.getDateTime(),x.getWeek()));
         JavaPairRDD<LocalDate, Tuple2<Integer, String>> result8= result6.join(result7).sortByKey();
         JavaRDD<Tuple2<Integer, String>> result9= result8.map(x->x._2());
        JavaPairRDD<String, Integer> prova_media = result9.mapToPair(x -> new Tuple2<>(x._2(), x._1())).reduceByKey((a, b) -> a + b).sortByKey();
        Integer average;

        for (Tuple2<String, Integer> i: prova_media.collect()){
            average=i._2()/7;
            System.out.println("settimana:  "+i._1()+" media: "+average);

        }


        /*for (Tuple2<Integer, String> i: result9.collect()){
            System.out.println(i);

        }*/


      /*  for (Tuple2<LocalDate, Tuple2<Integer, String>> i: result8.collect()){
            System.out.println(i);

        }*/


            sc.stop();


        }
    }
