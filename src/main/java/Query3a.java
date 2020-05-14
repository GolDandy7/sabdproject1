import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import KmeansMlibSpark.KMeansCompute;
import Parser.State;
import Parser.StateParser;
import utility.TrendLine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Query3a {

    private static String pathToFile = "src/dataset/covid19datiinternazionali_cleaned.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query3a");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> raws = sc.textFile(pathToFile);

        String firstRow = raws.first();
        String[] colnames = firstRow.split(",");

        //Definiamo un array di stringhe contente le date prese dai nomi delle colonne a partire dalla 4 colonna
        ArrayList<String> date_names = new ArrayList<>();
        for (int i = 4; i < colnames.length; i++)
            date_names.add(colnames[i]);

        //Creiamo un RDD di stringhe eliminando la prima riga contente i nomi delle colonne
        JavaRDD<String> covid_data3 = raws.filter(x -> !x.equals(firstRow));
        JavaRDD<State> rdd_state = covid_data3.map(line -> StateParser.parseCSV2(line));


        /*
        Prendiamo i dati singolarmente ottenendo un pair rdd del tipo:
         Tupla:<<Stato,Mese>,<Giorno,Valore>>
         */
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Integer, Integer>> pairRDD_total_flat = rdd_state.
                flatMapToPair(new PairFlatMapFunction<State, Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Iterator<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>>
                    call(State state) throws Exception {
                        ArrayList<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>> result_flat = new ArrayList<>();
                        for (int i = 0; i < date_names.size(); i++) {
                            Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>> temp =
                                    new Tuple2<>(new Tuple2<>(state.getState(), getMonth(date_names.get(i))),
                                            new Tuple2<>(getDay(date_names.get(i)), state.getSick_number().get(i)));
                            result_flat.add(temp);
                        }
                        return result_flat.iterator();
                    }
                });

        /*
         raggruppiamo per chiave : <stato,mese> ottenendo un iterable di <giorno,valore>
         */
        JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Integer>>> result = pairRDD_total_flat.groupByKey();

        /*
         dopo che abbiamo raggruppato per chiavi, otteniamo il trend generando un nuovo pair rdd cosi composto:
         Tuple2:<<Mese>,<Trend,Nome dello stato>>
         */
        JavaPairRDD<Integer, Tuple2<Double, String>> grouped = result.mapToPair(new PairFunction<Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Integer>>>, Integer, Tuple2<Double, String>>() {
            @Override
            public Tuple2<Integer, Tuple2<Double, String>>
            call(Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Integer>>> input) throws Exception {
                Integer month = input._1()._2();
                String state_name = input._1()._1();
                ArrayList<Double> values_per_month = new ArrayList<>();
                for (Tuple2<Integer, Integer> tupla : input._2()) {
                    values_per_month.add((double) tupla._2());
                }
                double res = TrendLine.getSlope(values_per_month, values_per_month.size());
                return new Tuple2<>(month, new Tuple2<>(res, state_name));
            }
        });

        /*
        Una volta ottenuti i trend per ogni mese, raggruppiamo per chiave ottenendo un paird rdd composto da:
        <Mese>,<Iterable<Trend,Nome dello stato>>
         */
        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> result_grouped = grouped.groupByKey();
        Integer finalI;

        //Definiamo un arrayList contente le tuple del tipo <Numero Mese, Lista di Tuple< Trend, Nome stato >> corrispettivi al mese
        ArrayList<Tuple2<Integer, ArrayList<Tuple2<Double, String>>>> list_top_per_month = new ArrayList<>();

        /*
        Calcoliamo i primi 50 stati in base al trend per ogni mese ottenendo una lista di liste composta da tuple cosi formate:
        <mese,arraylist<trend,stato>>
         */
        for (int i = 1; i <= grouped.countByKey().size(); i++) {
            ArrayList<Tuple2<Double, String>> list_tuple = new ArrayList<>();

            Integer finalI1 = i;
            JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> pairdRR_month = result_grouped.filter(x -> x._1().equals(finalI1));
            JavaPairRDD<Double, String> class_month = pairdRR_month.
                    flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Double, String>>>, Double, String>() {
                        @Override
                        public Iterator<Tuple2<Double, String>>
                        call(Tuple2<Integer, Iterable<Tuple2<Double, String>>> input) throws Exception {

                            ArrayList<Tuple2<Double, String>> result_flat2 = new ArrayList<>();
                            for (Tuple2<Double, String> tupla : input._2()) {
                                result_flat2.add(tupla);
                            }
                            return result_flat2.iterator();
                        }
                    });
            List<Tuple2<Double, String>> top = class_month.sortByKey(false).take(5);
            for (Tuple2<Double, String> iter : top) {
                list_tuple.add(iter);
            }
            list_top_per_month.add(new Tuple2<>(i, list_tuple));

        }

        /*
         prendiamo la lista e la trasformiamo in un Rdd
         */
        JavaRDD<Tuple2<Integer, ArrayList<Tuple2<Double, String>>>> input2 = sc.parallelize(list_top_per_month);

        /*
        prendiamo il nostro RDD creato precedentemente e lo trasformiamo in pair rdd cosi ottenuto:
        <mese><top 50 stati per mese>
         */
        JavaPairRDD<Integer, Tuple2<Double, String>> pair_final = input2.
                flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, ArrayList<Tuple2<Double, String>>>, Integer, Tuple2<Double, String>>() {
                    @Override
                    public Iterator<Tuple2<Integer, Tuple2<Double, String>>>
                    call(Tuple2<Integer, ArrayList<Tuple2<Double, String>>> row) throws Exception {
                        ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res3 = new ArrayList<>();
                        for (Tuple2<Double, String> k : row._2()) {
                            res3.add(new Tuple2<>(row._1(), k));
                        }
                        return res3.iterator();
                    }
                });

        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> temp4 = pair_final.groupByKey();

        /*  for ( Tuple2<Integer, Iterable<Tuple2<Double, String>>>tupla4 : temp4.collect()) {
            System.out.println(tupla4);


*/

        KMeansCompute.belongCluster(temp4);
       /* for(int j=1; j<=temp4.keys().collect().size();j++) {
            Integer ter=j;
            KMeansCompute.belongCluster(temp4.filter(x->x._1().equals(ter)));
        }*/
        /*for(Tuple2<Integer, ArrayList<Tuple2<Double, String>>> tupla3:input2.collect()){
            System.out.println(tupla3);
        } */
    }




    public static Integer getDay(String date){
        //mm/gg/aa
        String[] x=date.split("/");
        return Integer.parseInt(x[1]);
    }
    public static Integer getMonth(String date){
        String[] x=date.split("/");
        return Integer.parseInt(x[0]);
    }


}
