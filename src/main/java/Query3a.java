import KmeansMlibSpark.KMeansSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import entity.State;
import Parser.StateParser;
import utility.TrendLine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Query3a {

    private static final String pathToHDFS="hdfs://localhost:54310/dataset/covid19datiinternazionali.csv";
    private static final String putToHDFS="hdfs://localhost:54310/out/query3a";

    private static String pathToFile = "src/dataset/covid19datiinternazionali_cleaned.csv";
    private static int NUMERO_TOP_TREND=50;
    private static int MAX_ITERAZIONI=10;
    private static int NUMERO_CLUSTER=4;


    public static void main(String[] args) {

        long startTime;
        long endTime;

        ArrayList<Long> TotalTime= new ArrayList<>();

            startTime = System.nanoTime();

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
        Una volta ottenuti i trend per ogni mese, raggruppiamo per chiave ottenendo un pair rdd composto da:
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
                JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> pairdRR_month = result_grouped.
                        filter(x -> x._1().equals(finalI1));
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
                List<Tuple2<Double, String>> top = class_month.sortByKey(false).take(NUMERO_TOP_TREND);
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


            //++++++++++++++++++++++++++ START ALGORITMO MLLIB SPARK KMEANS+++++++++++++++++++++++++++++++++++++++

            List<Tuple2<Integer, Tuple2<String, Integer>>> to_file = new ArrayList<>();

            for (int num_iter = 1; num_iter <= temp4.keys().collect().size(); num_iter++) {
                int finalNum_iter = num_iter;
                JavaRDD<Vector> filtered = temp4.
                        filter(x -> x._1().equals(finalNum_iter)).
                        flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Double, String>>>, Vector>() {
                            @Override
                            public Iterator<Vector> call(Tuple2<Integer, Iterable<Tuple2<Double, String>>> input)
                                    throws Exception {
                                ArrayList<Vector> result5 = new ArrayList<>();
                                for (Tuple2<Double, String> tupla : input._2()) {
                                    Vector a = Vectors.dense(tupla._1());
                                    result5.add(a);
                                }
                                return result5.iterator();
                            }
                        });

                KMeansModel kMeansModel = KMeans.train(filtered.rdd(), NUMERO_CLUSTER, MAX_ITERAZIONI);
                KMeansSpark kMeansSpark = new KMeansSpark(kMeansModel);

                List<Iterable<Tuple2<Double, String>>> lista_punti = temp4.filter(x -> x._1().equals(finalNum_iter)).
                        map(x -> x._2()).collect();

                for (Iterable<Tuple2<Double, String>> lp : lista_punti) {
                    for (Tuple2<Double, String> k : lp) {
                        to_file.add(new Tuple2<>(num_iter, new Tuple2<>(k._2(), kMeansSpark.getkMeansModel().predict(Vectors.dense(k._1())))));
                    }
                }

            }


            JavaRDD<String> toParse3 = sc.parallelize(to_file).
                    map(x -> new String(x._1() + "," + x._2()._1() + "," + x._2()._2()));





            toParse3.saveAsTextFile(putToHDFS);;

            //++++++++++++++++++++++++++ END ALGORITMO MLLIB SPARK KMEANS+++++++++++++++++++++++++++++++++++++++
            sc.stop();
            endTime = System.nanoTime();
            TotalTime.add((endTime-startTime)/1_000_000_000);






        for(Long t:TotalTime)
            System.out.println(t +"secondi");
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
