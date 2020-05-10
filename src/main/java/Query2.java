import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator;
import scala.Tuple2;
import utility.State;
import utility.StateParser;
import utility.TrendLine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class Query2 {

    private static String pathToFile = "src/dataset/covid19datiinternazionali_cleaned.csv";
    private static Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

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
        ArrayList<String> date_names=new ArrayList<>();
        for(int i=4;i<colnames.length;i++)
            date_names.add(colnames[i]);


        JavaRDD<String> covid_data = raws.filter(x -> !x.equals(firstRow));
        JavaRDD<State> rdd_state=covid_data.map(line->StateParser.parseCSV2(line));
        /*// stampo rdd state per verificare se mi ha tolto la cumulativita
        for(State i:rdd_state.collect())
            System.out.println(i);*/

        JavaPairRDD<Double, String> pairRDD_trend = rdd_state.
                mapToPair(new PairFunction<State, Double, String>() {
                    @Override
                    public Tuple2<Double, String> call(State state) throws Exception {

                        ArrayList<Double> doubles = new ArrayList<>();
                        for (int i = 0; i < state.getSick_number().size(); i++)
                            doubles.add(Double.parseDouble(state.getSick_number().get(i)));
                        double slope = TrendLine.getSlope(doubles, state.getSick_number().size());
                        return new Tuple2<>(slope, state.getCountry());
                    }
                });


        List<Tuple2<Double, String>> pairTop = pairRDD_trend.sortByKey(false).take(10);

        JavaPairRDD<String,State> result1= rdd_state.mapToPair(x-> new Tuple2<>(x.getCountry(),x));

        JavaPairRDD <String, State> resultfilter= result1.filter(new Function<Tuple2<String, State>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, State> stringStateTuple2) throws Exception {
               for(Tuple2<Double,String> i: pairTop ) {
                   if (stringStateTuple2._1().equals(i._2()))
                       return true;
               }
                return false;
            }
        });

        for (Tuple2<String, State> i : resultfilter.collect()){
            System.out.println(i);
        }
        JavaPairRDD<String,ArrayList<Double>> continente= resultfilter.mapToPair(new PairFunction<Tuple2<String, State>, String, ArrayList<Double>>() {
            @Override
            public Tuple2<String, ArrayList<Double>> call(Tuple2<String, State> stringStateTuple2) throws Exception {
                String Continente;

                return null;
            }
        });


        //TODO: INVERTIRE CHIAVE VALORE
            /*for (Tuple2<Double, String> j : pairTop)
                System.out.println(j);*/

        sc.stop();


    }
}
