import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import Parser.State;
import Parser.StateParser;
import utility.TrendLine;

import java.time.Month;
import java.util.ArrayList;
import java.util.Iterator;

public class Query3 {
    private static String pathToFile = "src/dataset/covid19datiinternazionali_cleaned.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> raws = sc.textFile(pathToFile);
        String firstRow = raws.first();
        String[] colnames = firstRow.split(",");

        ArrayList<String> date_names=new ArrayList<>();
        for(int i=4;i<colnames.length;i++)
            date_names.add(colnames[i]);



        ArrayList<Month> month_array= StateParser.convertDatetoMonth(date_names);
        ArrayList<Integer> numerogiorni=StateParser.contagiorni(month_array);
      /*  for(int i=0; i<month_array.size();i++){
            System.out.println(month_array.get(i));
        }
        for(int i=0; i<numerogiorni.size();i++){
            System.out.println(numerogiorni.get(i));
        }*/
        JavaRDD<String> covid_data3 = raws.filter(x -> !x.equals(firstRow));
        JavaRDD<State> rdd_state=covid_data3.map(line->StateParser.parseCSV2(line));

        JavaPairRDD<String, ArrayList<Integer>> state_without_month= rdd_state.mapToPair(x-> new Tuple2<>(x.getState(),x.getSick_number()));

       /* for(Tuple2<String, ArrayList<Integer>> i:state_without_month.collect()){

                System.out.println(i);
        }*/

        // Tupla2<Italia, {0.0; 3.1; 0.3; 2.8, 0.23}>
        JavaPairRDD<String, ArrayList<Double>> arrayState= state_without_month.mapToPair(new PairFunction<Tuple2<String, ArrayList<Integer>>, String, ArrayList<Double>>() {
            @Override
            public Tuple2<String, ArrayList<Double>> call(Tuple2<String, ArrayList<Integer>> temp) throws Exception {
                ArrayList<Double> doubles =new ArrayList<>();
                ArrayList<Double> array_temp= new ArrayList<>();

                int l;
                Double slope;

                for (int i = 0; i<temp._2().size(); i++){
                    // month_array=[10,29,30,31,6]
                    for (int j=0; j<numerogiorni.size();j++){
                        for( l=i; l<(numerogiorni.get(j))+i; l++){
                            array_temp.add(Double.parseDouble(String.valueOf(temp._2().get(l))));
                        }
                        slope=TrendLine.getSlope(array_temp,array_temp.size());
                        doubles.add(slope);
                        array_temp.clear();
                        i=l;
                    }
                }

                return new Tuple2<>(temp._1(),doubles);
            }
        }).sortByKey();


        //stampa prova
        /*for(Tuple2<String, ArrayList<Double>> i:arrayState.collect()){

                System.out.println(i);
        }*/



        ArrayList<String> mesi= StateParser.raggruppa_mesi(month_array);

        //prova stampa mesi
       /* for(int j=0; j<mesi.size();j++){
            System.out.println(mesi.get(j));
        }*/


        JavaPairRDD<String, Tuple2<String,Double>> mese_trend= arrayState.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Double>>, String, Tuple2<String, Double>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Double>>> call(Tuple2<String, ArrayList<Double>> temp) throws Exception {
                ArrayList<Tuple2<String,Tuple2<String,Double>>> array_list= new ArrayList<>();
                for(int i=0; i<numerogiorni.size();i++){
                    Tuple2<String, Tuple2<String,Double>> tupla= new Tuple2<>(
                            mesi.get(i), new Tuple2<>(temp._1(),temp._2().get(i))
                    );
                    array_list.add(tupla);

                }

                return array_list.iterator();
            }
        });

        for(Tuple2<String, Tuple2<String,Double>> i: mese_trend.collect()){
            System.out.println(i);
        }


        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> raggruppamento_trend_mese = mese_trend.groupByKey();


        for (Tuple2<String, Iterable<Tuple2<String, Double>>> i: raggruppamento_trend_mese.collect()){
            System.out.println(i);
        }


       /* JavaPairRDD<Tuple2<Month,String>, Tuple2<Integer, Integer>> single_state_with_month=
              state_without_month.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>, Tuple2<Month, String>, Tuple2<Integer, Integer>>() {
          @Override
          public Iterator<Tuple2<Tuple2<Month, String>, Tuple2<Integer, Integer>>> call(Tuple2<String, ArrayList<Integer>> stringArrayListTuple2) throws Exception {
              ArrayList<Tuple2<Tuple2<Month, String>, Tuple2<Integer, Integer>>> res= new ArrayList<>();
              for(int i=0;i<month_array.size(); i++){
                  Tuple2<Tuple2<Month,String>,Tuple2<Integer,Integer>> temp=new Tuple2<>(new Tuple2<>(month_array.get(i),stringArrayListTuple2._1()),
                          new Tuple2<>(StateParser.pareserDate(date_names.get(i)),stringArrayListTuple2._2().get(i)));
                  res.add(temp);
              }
              return res.iterator();
          }
      });

*/





      /*JavaPairRDD<Tuple2<Month, String>, Iterable<Tuple2<Integer, Integer>>> raggruppamento= single_state_with_month.groupByKey();
     JavaPairRDD<Tuple2<Month, String>, Double> prova= raggruppamento.mapToPair(new PairFunction<Tuple2<Tuple2<Month, String>, Iterable<Tuple2<Integer, Integer>>>, Tuple2<Month, String>, Double>() {
          @Override
          public Tuple2<Tuple2<Month, String>, Double> call(Tuple2<Tuple2<Month, String>, Iterable<Tuple2<Integer, Integer>>> tuple2IterableTuple2) throws Exception {

             Double sum=0.0;
             while (tuple2IterableTuple2._2().iterator().hasNext())
                  sum += tuple2IterableTuple2._2().iterator().next()._2();

              return new Tuple2(tuple2IterableTuple2._1(),sum);
          }
      });

     JavaPairRDD<Double,Tuple2<Month,String>> rovescio= prova.mapToPair(x-> new Tuple2<>(x._2(),x._1())).sortByKey();
        List<Tuple2<Double, Tuple2<Month,String>>> pairTop = rovescio.sortByKey(false).take(10);


        for ( int i=0; i<pairTop.size();i++){
            System.out.println(pairTop.get(i));
        }

*/

   /* for( Tuple2<Tuple2<Month, String>, Iterable<Tuple2<Integer, Integer>>> i: raggruppamento.collect()){
            Integer sum=0;
            while(i._2().iterator().hasNext())
                sum += i._2().iterator().next()._2();

           //Tuple2<Integer,Integer> tupla=i._2().iterator().next();
           System.out.println(sum);
        }*/

         /* for( Tuple2<Tuple2<Month,String>, Tuple2<Integer, Integer>>i: single_state_with_month.collect()){
            System.out.println(i);
        }*/






        sc.stop();

    }
}
