import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import utility.Outlet;
import utility.OutletParser;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class Query1 {

    private static final String pathToFile = "src/dataset/covid19datinazionali.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logLines = sc.textFile(pathToFile);
        String firstRow = logLines.first();
        JavaRDD<String> covid1 = logLines.filter(x -> !x.equals(firstRow));

        /*
         Parsato il csv
        */
        JavaRDD<Outlet> outlets =
                covid1.map(
                        line -> OutletParser.parseCSV(line));

        /*
         Creazione di due RDD: un RDD normale e uno contentente i dati shiftati di un giorno
        */
        JavaPairRDD<LocalDate,Integer> pairRDD_healed= outlets.
                mapToPair(x -> new Tuple2<>(x.getDateTime(),x.getGuariti()));
        JavaPairRDD<LocalDate,Integer> pairRDD_healed_shifted= outlets.
                mapToPair(x ->new Tuple2<>(x.getDateTime().plusDays(1), x.getGuariti()));

        /*
         Creato RDD contentente la coppia chiave,valore contentente valori non cumulativi.
         */
        JavaPairRDD<LocalDate, Integer> pairRDD_daily_healed = pairRDD_healed_shifted.
                rightOuterJoin(pairRDD_healed).
                mapToPair(new PairFunction<Tuple2<LocalDate, Tuple2<Optional<Integer>, Integer>>, LocalDate, Integer>() {
                    @Override
                    public Tuple2<LocalDate, Integer> call(Tuple2<LocalDate, Tuple2<Optional<Integer>, Integer>> row) throws Exception {
                        if(row._2()._1().isPresent()) {
                            Integer diff=row._2()._2() - row._2()._1().get();
                            return new Tuple2<>(row._1(),diff);
                        }
                        return new Tuple2<>(row._1(),row._2()._2());
                    }
                });

        /*
         Creazione di un RDD (map reduce) contentente la somma dei valori per settimana
        */
        JavaPairRDD<String,Integer> pairRDD_week_healed=pairRDD_daily_healed.
                mapToPair(x-> new Tuple2<>(getWeek(x._1().toString()),x._2())).
                reduceByKey((a,b)->a+b);
        //TODO: essere robusti sul numero di giorni per settimana, non è detto che sia sempre diviso 7
        /*
         Creazione di un RDD contentente la media settimanale dei guariti
         */
        JavaPairRDD<String,Double> pairRDD_avg_healed=pairRDD_week_healed.
                mapToPair(x-> new Tuple2<>(x._1(),(double)x._2()/7));

        for (Tuple2<String, Double> avg: pairRDD_avg_healed.collect()){
            System.out.println(avg);

        }
        sc.stop();
       }



    public static String getWeek(String data) throws ParseException {

        System.out.println(data);
        //2020-02-24T18:00:00
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        String w= "W"+week;


        return w;
    }
}
