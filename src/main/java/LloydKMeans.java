import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LloydKMeans {

    public static void Naive(JavaRDD<Iterable<Tuple2<Double,String>>> input, Integer mese){

        ArrayList<Double> centroide =new ArrayList<>();
        centroide.add(6.0);
        centroide.add(3.32);
        centroide.add(0.29);
        centroide.add(-1.9);
        //centroide.add(200.0);
       // centroide.add((-10.0));

       /* for(Iterable<Tuple2<Double,String>> p:input.collect()){
            System.out.println(p+" "+ mese);
        }*/

        //in temp_1 mi tornano i cluster all'inizio < cluster=1 ,<ternd=3,4 , stato=Italia>>
        JavaPairRDD<Integer, Tuple2<Double, String>> cluster_1 = null;
        for(int iter=0; iter<20; iter++) {
//            System.out.println(mese+" ******ITERAZIONE****** "+ iter + "***************\n");
             cluster_1 = input.flatMapToPair(new PairFlatMapFunction<Iterable<Tuple2<Double, String>>, Integer, Tuple2<Double, String>>() {
                @Override
                public Iterator<Tuple2<Integer, Tuple2<Double, String>>> call(Iterable<Tuple2<Double, String>> tuple2s) throws Exception {
                    Double min;
                    Double z;
                    Integer cluster;
                    ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();
                    for (Tuple2<Double, String> t : tuple2s) {
                        min = Math.abs(centroide.get(0) - t._1());
                        cluster = 0;
                        for (int i = 0; i < centroide.size(); i++) {
                            z = Math.abs(centroide.get(i) - t._1());
                            if (min > z) {
                                min = z;
                                cluster = i;
                            }
                        }
                        res.add(new Tuple2<>(cluster, t));

                    }
                    return res.iterator();
                }
            });
      /* for(Tuple2<Integer,Tuple2<Double,String>> p:cluster_1.collect()){
            System.out.println(mese +" cluster "+p._1()+" "+ p._2()._1());
        }*/


            //Riduco tutti i punti del cluster j
            //<cluster=1, trend=3,4>
            // sommare gli elementi/numero di elementi
            JavaPairRDD<Integer, Double> cluster_2 = cluster_1.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1()));
            JavaPairRDD<Integer, Double> centroide_nuovo = cluster_2.reduceByKey((a, b) -> a + b);
            JavaPairRDD<Integer, Integer> cluster_count = cluster_1.mapToPair(x -> new Tuple2<>(x._1(), 1));
            JavaPairRDD<Integer, Tuple2<Double, Integer>> join_fox0 = centroide_nuovo.join(cluster_count);
            JavaPairRDD<Integer, Double> centroide_nuovo_final = join_fox0.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1() / x._2()._2()));

            for (Tuple2<Integer, Double> p : centroide_nuovo_final.collect()) {
                for (int i = 0; i < centroide.size(); i++)
                    if (p._1() == i) {
                        centroide.set(i, p._2());
                    }
            }
/*
            for (int i = 0; i < centroide.size(); i++) {
                System.out.println(centroide.get(i));
            }*/
        }
        for(Tuple2<Integer, Tuple2<Double, String>> t: cluster_1.collect()){
            System.out.println("Mese: "+mese+ " stato"+ t._2()._2()+ "trend:" + t._2()._1() + " **Cluster: "+ t._1());
        }
        /*for(Tuple2<Integer, Double> t:centroide_nuovo_final.collect()){
            System.out.println("mese"+ mese+ " " + t);
        }*/

        /*centroide.clear();
            for (Tuple2<Integer, Double> t : map_final3.collect()) {
                centroide.add(t._2());
            }*/

        //adesso i cluster non mi servono più quindi metto una chiava generica
        // array[centroidi nuovi], trend
        /*JavaPairRDD<Integer,Double> generic=centroide_nuovo_final.mapToPair(x->new Tuple2<>(1,x._2()));
        JavaPairRDD<Integer, Iterable<Double>> gr_1 = generic.groupByKey().distinct();
        JavaPairRDD<Integer,Tuple2<Integer,Tuple2<Double,String>>> fox_1=cluster_1.mapToPair(x-> new Tuple2<>(1,x));
        // <1, stato=italia trend=3,4 array_centroidi_nuovi[]>
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, String>>, Iterable<Double>>> joinfox_1 = fox_1.join(gr_1).distinct();


       *//*for(Tuple2<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, String>>, Iterable<Double>>> pippo:joinfox_1.collect()){
            System.out.println(pippo);

        }*//*
        JavaPairRDD<ArrayList<Double>,Tuple2<Double,String>> map2_1=joinfox_1.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, String>>, Iterable<Double>>>, ArrayList<Double>, Tuple2<Double, String>>() {
            @Override
            public Tuple2<ArrayList<Double>, Tuple2<Double, String>> call(Tuple2<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, String>>, Iterable<Double>>> x) throws Exception {
                ArrayList<Double> d= new ArrayList<>();
                for(Double t:x._2()._2()){
                    d.add(t);
                }
                return new Tuple2<>(d,x._2()._1()._2());
            }
        });



            // adesso mi calcolco di nuovo i cluster con  i centroidi che sono stati trovati precedentemente
            // <cluster, trend, stato>
            JavaPairRDD<Integer, Tuple2<Double, String>> map_final = map2_1.mapToPair(new PairFunction<Tuple2<ArrayList<Double>, Tuple2<Double, String>>, Integer, Tuple2<Double, String>>() {
                @Override
                public Tuple2<Integer, Tuple2<Double, String>> call(Tuple2<ArrayList<Double>, Tuple2<Double, String>> input) throws Exception {
                    Double min = Math.abs(input._2()._1() - input._1().get(0));
                    Double z = 0.0;
                    Integer cl = 0;

                    for (int i = 0; i < input._1().size(); i++) {
                        z = Math.abs(input._2()._1() - input._1().get(i));
                        if (min > z) {
                            min = z;
                            cl = i;
                        }

                    }
                    return new Tuple2<>(cl, input._2());

                }
            });

            //adesso il cluster non mi serve perchè devo rifare la reduce
            JavaPairRDD<Integer, Double> map_final_2 = map_final.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1()));

            *//*for(Tuple2<Integer, Double> p:map_final_2.collect()){
                System.out.println(p+" "+ mese);

            }*//*

            JavaPairRDD<Integer, Integer> cont_map = map_final.mapToPair(x -> new Tuple2<>(x._1(), 1));

            JavaPairRDD<Integer, Integer> cont_red = cont_map.reduceByKey((a, b) -> a + b);

            JavaPairRDD<Integer, Double> red_final = map_final_2.reduceByKey((a, b) -> a + b);

            JavaPairRDD<Integer, Tuple2<Double, Integer>> joinfox_2 = red_final.join(cont_red);

            //adesso mi da cluster e valore il nuovo centroide uj
            JavaPairRDD<Integer, Double> map_final3 = joinfox_2.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1() / x._2()._2()));



            //adesso creo un RDD con chiave  il trend e valore lo stato
            JavaPairRDD<Double, String> trend_state = cluster_1.mapToPair(x -> new Tuple2<>(x._2()._1(), x._2()._2()));

            //creo un RDD con chiave il trend e valore il cluster
            JavaPairRDD<Double, Integer> trend_cluster = map_final_2.mapToPair(x -> new Tuple2<>(x._2(), x._1()));

            //creo un RDD  stato trend e cluster
            JavaPairRDD<Double, Tuple2<String, Integer>> join_fox3 = trend_state.join(trend_cluster);
            *//*for(Tuple2<Double, Tuple2<String, Integer>> t: join_fox3.collect()){
                System.out.println("mese: "+ mese+" stato "+ t._2()._1()+" trend: "+ t._1()+ "      cluster "+ t._2()._2());

            }
*//*
            JavaRDD<Tuple3<String, Double, Integer>> final_res = join_fox3.map(x -> new Tuple3<>(x._2()._1(), x._1(), x._2()._2()));*/
        }




}
