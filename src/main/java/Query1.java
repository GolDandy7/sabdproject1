import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import producer.Producer;
import scala.Tuple2;
import entity.Outlet;
import Parser.OutletParser;
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
        JavaRDD<String> rdd_covid_rows = sc.textFile(pathToFile);
        //elimino la prima riga contentente l'header del csv
        String firstRow = rdd_covid_rows.first();
        JavaRDD<String> rdd_covid_rows_withoutFirst = rdd_covid_rows.filter(x -> !x.equals(firstRow));

        /*
         Parsato il csv
        */
        JavaRDD<Outlet> outlets =
                rdd_covid_rows_withoutFirst.map(
                        line -> OutletParser.parseCSV(line));

        /*
         Creazione di due RDD: un RDD normale e uno contentente i dati guariti shiftati di un giorno
        */
        JavaPairRDD<LocalDate,Integer> pairRDD_healed= outlets.
                mapToPair(x -> new Tuple2<>(x.getDateTime(),x.getHealed()));
        JavaPairRDD<LocalDate,Integer> pairRDD_healed_shifted= outlets.
                mapToPair(x ->new Tuple2<>(x.getDateTime().plusDays(1), x.getHealed()));

         /*
         Creazione di due RDD: un RDD normale e uno contentente i dati dei tamponi shiftati di un giorno
        */
        JavaPairRDD <LocalDate , Integer> pairRDD_swabs= outlets.
                mapToPair( x->  new Tuple2<>(x.getDateTime(), x.getSwabs()));
        JavaPairRDD <LocalDate , Integer> pairRDD_shifted= outlets.
                mapToPair( x->  new Tuple2<>(x.getDateTime().plusDays(1), x.getSwabs()));

        // RDD result1-> 2020/02/24 tupla<1, 3>, mentre result1shift-> 2020/02/25 tupla <1,3>
        JavaPairRDD<LocalDate, Tuple2<Integer, Integer>> pairRDD_join_sh = pairRDD_healed.join(pairRDD_swabs);
        JavaPairRDD<LocalDate, Tuple2<Integer, Integer>> pairRDD_join_sh_shifted = pairRDD_healed_shifted.join(pairRDD_shifted);

        /* RDD che contiene i dati non cumulativi di Guariti e Tamponi*/
        JavaPairRDD<LocalDate, Tuple2<Integer, Integer>> pairRDD_daily_sh = pairRDD_join_sh_shifted.
                rightOuterJoin(pairRDD_join_sh).
                mapToPair(new PairFunction<Tuple2<LocalDate, Tuple2<Optional<Tuple2<Integer, Integer>>, Tuple2<Integer,
                        Integer>>>, LocalDate, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<LocalDate, Tuple2<Integer, Integer>> call(Tuple2<LocalDate,
                            Tuple2<Optional<Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> lrow) throws Exception {
                        if(lrow._2()._1().isPresent()){
                            Integer diff1= lrow._2()._2()._1() - lrow._2()._1().get()._1();
                            Integer diff2= lrow._2()._2()._2() - lrow._2()._1().get()._2();
                            return new Tuple2<>(lrow._1(), new Tuple2<>(diff1,diff2));
                        }
                        return new Tuple2<>(lrow._1(), new Tuple2<>(lrow._2()._2()._1(),lrow._2()._2()._2()));
                    }
                });
        /*
        RDD con numero settimana e somme dei valori guariti e valori tamponi
            esempio : <w1,lista guariti,lista tamponi>
         */
        JavaPairRDD<String,Tuple2<Integer, Integer>> pairRDD_week_sum_sh= pairRDD_daily_sh.
                mapToPair(x-> new Tuple2<>(getWeek(x._1().toString()),x._2()));
        /*
        RDD con numero settimana e somme dei valori guariti e valori tamponi
            esempio : <w1,somma guariti,somma tamponi>
         */
        JavaPairRDD<String,Tuple2<Integer, Integer>> pairRDD_sum_result=pairRDD_week_sum_sh.
                reduceByKey((a,b)-> new Tuple2<>(a._1() + b._1(), a._2()+ b._2()));

        /*
        RDD che conta il numero di giorni della settimana l'ultima deve essere di 3 giorni
        */
        JavaPairRDD <String, Integer > pairRDD_week_count= pairRDD_week_sum_sh.
                mapToPair( x-> new Tuple2<>(x._1(), 1)).reduceByKey((a,b)-> a+b);
        /*
        Unisco i due RDD per poter effettuare correttamente la media
         */
        JavaPairRDD<String, Tuple2<Tuple2<Integer, Integer>, Integer>> pairRDD_join_week_counted = pairRDD_sum_result.
                join(pairRDD_week_count);

        /* RDD con numero settimana e medie dei valori di guariti e valori di tamponi
         */
        JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, Integer>> pairRDD_final_results = pairRDD_join_week_counted.
                mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>(new Tuple2<>((double)x._2()._1()._1() / x._2()._2(),
                        (double)x._2()._1()._2() / x._2()._2()), x._2()._2()))).sortByKey();

        for (Tuple2<String, Tuple2<Tuple2<Double, Double>, Integer>> i: pairRDD_final_results.collect()){

            System.out.println("Settimana:" + i._1() +" numero di giorni:"+ i._2()._2() +" numero guariti medio :"+
                    i._2()._1()._1() + " numero medio tamponi: "+ i._2()._1()._2());

        }

        //pairRDD_final_results.saveAsTextFile("src/dataset/output1");

        /*JavaRDD<String> prova = pairRDD_final_results.map( i-> new String("Settimana:" + i._1() +" numero di giorni:"+ i._2()._2() +" numero guariti medio :"+
                i._2()._1()._1() + " numero medio tamponi: "+ i._2()._1()._2()));*/
        JavaRDD<String> toParse=pairRDD_final_results.
                map(x->new String(x._1()+","+x._2()._1()._1()+","+x._2()._1()._2()+","+x._2()._2()));

        //toParse.saveAsTextFile("pathOutputQuery1");
        for(String s: toParse.collect()){
            System.out.println(s);
        }

        //Producer producer = new Producer();
        //producer.sendToHDFS(prova.collect(),"result1");
        /*Producer producer1= new Producer();
        producer1.sendToHbase(prova.collect(), "hbase1");*/


        sc.stop();
    }
    // TODO: creare una classe con tutte le utils
    public static String getWeek(String data) throws ParseException {
        //2020-02-24
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        String w= "W"+week;
        return w;
    }
}
