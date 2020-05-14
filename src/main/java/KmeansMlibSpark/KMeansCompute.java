package KmeansMlibSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.mllib.linalg.Vectors;
import java.util.ArrayList;

public class KMeansCompute {

    private static Integer num_cluster=4;
    private static Integer num_iterazioni=20;
    public static void
    belongCluster(JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> integerIterableJavaPairRDD){

        //Prendiamo i valori contenuti nel RDD
        JavaRDD<Iterable<Tuple2<Double, String>>> value_per_month = integerIterableJavaPairRDD.values();

        //MI SERVE PER LA UNION?
        JavaRDD<Integer> chiave = integerIterableJavaPairRDD.keys();
        /*//System.out.println(chiave.collect());
        for(Iterable<Tuple2<Double, String>> x:value_per_month.collect()){
            System.out.println("Dopo il .values:"+x);
        }*/

        //Mappo i valori dell'RDD in un RDD formato da array di valori e array di nomi degli stati conservandone l'ordine
        JavaRDD<Tuple2<Double[],ArrayList<String>>> rdd=value_per_month.
                map(new Function<Iterable<Tuple2<Double, String>>, Tuple2<Double[], ArrayList<String>>>() {
            @Override
            public Tuple2<Double[], ArrayList<String>>
            call(Iterable<Tuple2<Double, String>> tuple2s) throws Exception {
                ArrayList<Double> doubles=new ArrayList<>();
                ArrayList<String> strings=new ArrayList<>();
                for(Tuple2<Double,String> temp:tuple2s){
                    doubles.add(temp._1());
                    strings.add(temp._2());
                }
                //double [] temp=doubles.stream().mapToDouble(Double::doubleValue).toArray();
                Double [] temp=doubles.toArray(new Double[0]);

                return new Tuple2<>(temp,strings);
            }
        });


        JavaRDD<Vector> parsedouble = rdd.map(new Function<Tuple2<Double[], ArrayList<String>>, Vector>() {
            @Override
            public Vector call(Tuple2<Double[], ArrayList<String>> arrayListTuple2) throws Exception {
                double[] values = new double[arrayListTuple2._1().length];
                for (int i = 0; i < arrayListTuple2._1().length; i++) {
                    values[i] = Double.parseDouble(String.valueOf(arrayListTuple2._1()[i]));
                }
                return Vectors.dense(values);
            }
        });

        parsedouble.cache();
        KMeansModel clusters = KMeans.train(parsedouble.rdd(), num_cluster, num_iterazioni);

        /*for(Tuple2<Double[], ArrayList<String>> pippo : rdd.collect()){
            for(int index=0;index<pippo._1().length;index++)
                System.out.println(pippo._1()[index]);
            System.out.println("\n");
        }*/

        //JavaRDD<Vector<Double>> rdd_vector=rdd.map(x->x._1());
        //KMeansModel clusters = KMeans.train(,num_cluster,num_iterazioni);
       /* JavaRDD<Tuple2<ArrayList<Double>,ArrayList<String>>> rdd=value_per_month.map(new Function<Iterable<Tuple2<Double, String>>, Tuple2<ArrayList<Double>, ArrayList<String>>>() {
            @Override
            public Tuple2<ArrayList<Double>, ArrayList<String>>
            call(Iterable<Tuple2<Double, String>> tuple2s) throws Exception {
                ArrayList<Double> doubles=new ArrayList<>();
                ArrayList<String> strings=new ArrayList<>();
                for(Tuple2<Double,String> temp:tuple2s){
                    doubles.add(temp._1());
                    strings.add(temp._2());
                }
                return new Tuple2<>(doubles,strings);
            }
        });*/


    }
}
