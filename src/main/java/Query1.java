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



        //per pulire l'RDD dai dati cumulativi
        JavaRDD<Tuple3<LocalDate, Integer, Integer>> result11 = outlets.map(x -> new Tuple3<LocalDate, Integer, Integer>
                (x.getDateTime(), x.getGuariti(), x.getTamponi()));
        JavaRDD<Tuple3<LocalDate, Integer, Integer>> result3 = outlets.map(x -> new Tuple3<LocalDate, Integer, Integer>
                (x.getDateTime().minusDays(1), x.getGuariti(), x.getTamponi()));

        JavaPairRDD<LocalDate,Integer> result5= outlets.mapToPair(x ->new Tuple2<>(x.getDateTime(), x.getGuariti()));
        JavaPairRDD<LocalDate,Integer> result4= outlets.mapToPair(x ->new Tuple2<>(x.getDateTime().minusDays(1), x.getGuariti()));
//        JavaPairRDD<LocalDate,Integer> result16= result4.join(x-> );


        String secondRow = covid1.first();
        JavaRDD<String> covid2 = covid1.filter(x -> !x.equals(secondRow));
        JavaRDD<Outlet> outlet2 =
                covid2.map(
                        line -> OutletParser.parseCSV(line));
        JavaRDD<Tuple3<String, Integer, Integer>> result2 = outlet2.map(x -> new Tuple3<String, Integer, Integer>
                (x.getWeek(), x.getGuariti(), x.getTamponi()));


        JavaPairRDD<String, Integer> coppia = outlets.mapToPair(x -> new Tuple2<>(x.getWeek(), x.getGuariti())).reduceByKey((a, b) -> a + b);

/*
        Integer average;
        for (Tuple2<String,Integer> i: coppia.collect()){
            average=i._2()/7;
            System.out.println("Settimana: "+i._1()+"  media: "+average);

        }*/

        for (Tuple3<LocalDate, Integer, Integer> i : result3.collect()) {

            System.out.println(i);
        }

    /*    }
        for (Tuple3<String, Integer, Integer> i : result2.collect()){

            System.out.println(i);

        }*/

            sc.stop();


        }
    }
