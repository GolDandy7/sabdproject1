import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import Parser.RegionParser;
import entity.State;
import Parser.StateParser;
import utility.Utilities;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class Query2 {


    private static final String pathToHDFS="hdfs://localhost:54310/dataset/covid19datiinternazionali.csv";
    private static final String putToHDFS="hdfs://localhost:54310/out/query2";
    private static final String pathregionHDFS= "hdfs://localhost:54310/dataset/country_region.csv";

    //in locale
    private static String pathToFile = "src/dataset/covid19datiinternazionali_cleaned.csv";
    private static final String pathregion= "src/dataset/country_region.csv";
    private static final String putLocal = "src/out/query2";

    private static Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        long startTime;
        long endTime;
        long TotalTime;


        startTime = System.nanoTime();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> raws = sc.textFile(pathToFile);
        /*
            1. tolgo la prima riga del file contenente il nome delle colonne
            2. splitto la riga per trovare il nome delle colonne
         */
        String firstRow = raws.first();
        String[] colnames = firstRow.split(",");
        ArrayList<String> date_names = new ArrayList<>();
        for (int i = 4; i < colnames.length; i++)
            date_names.add(colnames[i]);


        JavaRDD<String> covid_data = raws.filter(x -> !x.equals(firstRow));
        JavaRDD<State> rdd_state = covid_data.map(line -> StateParser.parseCSV2(line));


        //RDD del csv region nostro
        JavaRDD<String> rddregion = sc.textFile(pathregion);
        String firstRowRegion = rddregion.first();
        JavaRDD<String> rdd_regio_withoutFirst = rddregion.filter(x -> !x.equals(firstRowRegion));
        JavaPairRDD<String, String> rddPair_region = rdd_regio_withoutFirst.
                mapToPair(x -> new Tuple2<>(RegionParser.parseCSVRegion(x).getCountryRegion(), RegionParser.parseCSVRegion(x).getRegion()));


        //fine RDD region

        JavaPairRDD<Double, String> pairRDD_trend = rdd_state.
                mapToPair(new PairFunction<State, Double, String>() {
                    @Override
                    public Tuple2<Double, String> call(State state) throws Exception {

                        ArrayList<Double> doubles = new ArrayList<>();
                        for (int i = 0; i < state.getSick_number().size(); i++)
                            doubles.add(Double.valueOf(state.getSick_number().get(i)));
                        double slope = Utilities.getSlope(doubles, state.getSick_number().size());
                        return new Tuple2<>(slope, state.getState());
                    }
                });


        List<Tuple2<Double, String>> pairTop = pairRDD_trend.sortByKey(false).take(100);


        JavaPairRDD<String, State> result1 = rdd_state.mapToPair(x -> new Tuple2<>(x.getState(), x));
        JavaPairRDD<String, State> resultfilter = result1.filter(new Function<Tuple2<String, State>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, State> stringStateTuple2) throws Exception {
                for (Tuple2<Double, String> i : pairTop) {
                    if (stringStateTuple2._1().equals(i._2())) {
                        return true;
                    }
                }
                return false;
            }
        });



        JavaPairRDD<String, State> pairRDD_state_country = resultfilter.mapToPair(x -> new Tuple2<>(x._2().getCountry(), x._2()));

        JavaPairRDD<String, Tuple2<State, String>> rdd_continents = pairRDD_state_country.join(rddPair_region);

        JavaPairRDD<String, ArrayList<Integer>> rdd_region_final = rdd_continents.
                mapToPair(x -> new Tuple2<>(x._2()._2(), x._2()._1().getSick_number()));



        JavaPairRDD<String, ArrayList<Integer>> pairRDD_sum = rdd_region_final.
                reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
                    @Override
                    public ArrayList<Integer> call(ArrayList<Integer> integers, ArrayList<Integer> integers2) throws Exception {
                        ArrayList<Integer> sum_result = new ArrayList<>();
                        for (int i = 0; i < integers.size(); i++)
                            sum_result.add(integers.get(i) + integers2.get(i));
                        return sum_result;
                    }
                });

        ArrayList<String> week = StateParser.convertDatetoWeek(date_names);
        // <continente,week>,somma valore per quel giorno per continente
        JavaPairRDD<Tuple2<String, String>, Integer> pair_flat = pairRDD_sum.
                flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>, Tuple2<String, String>, Integer>() {
                    @Override
                    public Iterator<Tuple2<Tuple2<String, String>, Integer>>
                    call(Tuple2<String, ArrayList<Integer>> stringArrayListTuple2) throws Exception {
                        ArrayList<Tuple2<Tuple2<String, String>, Integer>> result_flat = new ArrayList<>();

                        for (int i = 0; i < week.size(); i++) {
                            Tuple2<Tuple2<String, String>, Integer> temp = new Tuple2<>(
                                    new Tuple2<>(stringArrayListTuple2._1(), week.get(i)), stringArrayListTuple2._2().get(i));
                            result_flat.add(temp);
                        }
                        return result_flat.iterator();
                    }
                });




        JavaPairRDD<Tuple2<String, String>, Integer> unitario = pair_flat.mapToPair(x -> new Tuple2<>(x._1(), 1));
        JavaPairRDD<Tuple2<String, String>, Integer> count = unitario.reduceByKey((a, b) -> a + b);
        JavaPairRDD<Tuple2<String, String>, Integer> arrayMax = pair_flat.reduceByKey((a, b) -> Math.max(a, b));
        JavaPairRDD<Tuple2<String, String>, Integer> arrayMin = pair_flat.reduceByKey((a, b) -> Math.min(a, b));
        JavaPairRDD<Tuple2<String, String>, Integer> reduced_flat = pair_flat.reduceByKey((a, b) -> a + b);
        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> joinres = reduced_flat.join(count);
        JavaPairRDD<Tuple2<String, String>, Double> average = joinres.mapToPair(x -> new Tuple2<>(x._1(), Double.parseDouble(String.valueOf(x._2()._1() / x._2()._2()))));

        JavaPairRDD<Tuple2<String, String>, Double> pairRDD_dev_std = average.join(pair_flat).
                mapToPair(x -> new Tuple2<>(x._1(), Math.pow(x._2()._1() - x._2()._2(), 2))).
                reduceByKey((a, b) -> a + b).
                join(count).
                mapToPair(x -> new Tuple2<>(x._1(), Math.sqrt(x._2()._1() / x._2()._2())));



        JavaPairRDD<Tuple2<String, String>, Tuple2<Tuple2<Tuple2<Integer, Integer>, Double>, Double>> result_final =
                (arrayMax.join(arrayMin)).join(average).join(pairRDD_dev_std);


        JavaRDD<String> toparse2 = result_final.map(x -> new String(x._1()._1() +
                "," + x._1()._2() + "," + x._2()._1()._1()._1() + "," + x._2()._1()._1()._2() + "," + x._2()._1()._2() +
                "," + x._2()._2()));

        //toparse2.saveAsTextFile(putToHDFS);
        toparse2.saveAsTextFile(putLocal);

        sc.stop();
        endTime = System.nanoTime();
        TotalTime = endTime - startTime;
        System.out.println("Query2 time: " + TotalTime / 1_000_000_000 + " secondi");

    }
}

