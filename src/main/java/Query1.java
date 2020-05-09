import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
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

        //elimino la prima riga contentente l'header del csv
        String firstRow = logLines.first();
        JavaRDD<String> covid1 = logLines.filter(x -> !x.equals(firstRow));

        /*
         Parsato il csv
        */
        JavaRDD<Outlet> outlets =
                covid1.map(
                        line -> OutletParser.parseCSV(line));

        /*
         Creazione di due RDD: un RDD normale e uno contentente i dati guariti shiftati di un giorno
        */
        JavaPairRDD<LocalDate,Integer> pairRDD_healed= outlets.
                mapToPair(x -> new Tuple2<>(x.getDateTime(),x.getGuariti()));
        JavaPairRDD<LocalDate,Integer> pairRDD_healed_shifted= outlets.
                mapToPair(x ->new Tuple2<>(x.getDateTime().plusDays(1), x.getGuariti()));

         /*
         Creazione di due RDD: un RDD normale e uno contentente i dati dei tamponi shiftati di un giorno
        */
        JavaPairRDD <LocalDate , Integer> tamponi= outlets.mapToPair( x->  new Tuple2<>(x.getDateTime(), x.getTamponi()));
        JavaPairRDD <LocalDate , Integer> tamponishif= outlets.mapToPair( x->  new Tuple2<>(x.getDateTime().plusDays(1), x.getTamponi()));



        // RDD result1-> 2020/02/24 tupla<1, 3>, mentre result1shift-> 2020/02/25 tupla <1,3>
        JavaPairRDD<LocalDate, Tuple2<Integer, Integer>> result1 = pairRDD_healed.join(tamponi);
        JavaPairRDD<LocalDate, Tuple2<Integer, Integer>> result1shift = pairRDD_healed_shifted.join(tamponishif);




        /* RDD che contiene i dati non cumulativi di Guariti e Tamponi*/

        JavaPairRDD<LocalDate, Tuple2<Integer, Integer>> result2 = result1shift.
                rightOuterJoin(result1).
                mapToPair(new PairFunction<Tuple2<LocalDate, Tuple2<Optional<Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>>, LocalDate, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<LocalDate, Tuple2<Integer, Integer>> call(Tuple2<LocalDate, Tuple2<Optional<Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> lrow) throws Exception {
                        if(lrow._2()._1().isPresent()){
                            Integer diff1= lrow._2()._2()._1() - lrow._2()._1().get()._1();
                            Integer diff2= lrow._2()._2()._2() - lrow._2()._1().get()._2();
                            return new Tuple2<>(lrow._1(), new Tuple2<>(diff1,diff2));


                        }

                        return new Tuple2<>(lrow._1(), new Tuple2<>(lrow._2()._2()._1(),lrow._2()._2()._2()));
                    }
                });

        /* RDD con numero settimana e somme dei valori guariti e valori tamponi

        * */

        JavaPairRDD<String,Tuple2<Integer, Integer>> result3= result2.
                mapToPair(x-> new Tuple2<>(getWeek(x._1().toString()),x._2()));
        JavaPairRDD<String,Tuple2<Integer, Integer>> result4=result3.
                reduceByKey((a,b)-> new Tuple2<>(a._1() + b._1(), a._2()+ b._2()));

        /* RDD che conta il numero di giorni della settimana l'ultima deve essere di 3 giorni
        * */
        JavaPairRDD <String, Integer > resultcount= result3.mapToPair( x-> new Tuple2<>(x._1(), 1)).reduceByKey((a,b)-> a+b);

        JavaPairRDD<String, Tuple2<Tuple2<Integer, Integer>, Integer>> resultfinal = result4.join(resultcount);

        /* RDD con numero settimana e medie dei valori di guariti e valori di tamponi
         * */
        JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, Integer>> avgresultfinal = resultfinal.mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>(new Tuple2<>((double)x._2()._1()._1() / x._2()._2(), (double)x._2()._1()._2() / x._2()._2()), x._2()._2()))).sortByKey();



         for (Tuple2<String, Tuple2<Tuple2<Double, Double>, Integer>> i: avgresultfinal.collect()){

            System.out.println("Settimana:" + i._1() +" numero di giorni:"+ i._2()._2() +" numero guariti medio :"+ i._2()._1()._1() + " numero medio tamponi: "+ i._2()._1()._2());

        }


        sc.stop();
       }



    public static String getWeek(String data) throws ParseException {

        //2020-02-24T18:00:00
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        String w= "W"+week;


        return w;
    }
}
