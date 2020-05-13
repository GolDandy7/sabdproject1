package utility;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.ArrayList;

import static org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType.slope;

public class TrendLine {
    public static double getSlope(ArrayList<Double> items,int size) {
        // creating regression object, passing true to have intercept term
        SimpleRegression simpleRegression = new SimpleRegression(true);
        // passing data to the model
        // model will be fitted automatically by the class
        for( int i=0;i<size;i++)
         simpleRegression.addData(i,items.get(i));
        // querying for model parameters
        return  simpleRegression.getSlope();
    }
    public static double getDeviazione(ArrayList<Double> items,int size){
       double sum=0.0;
       double sum2=0.0;
       double media;
       double dev;
        for(int i=0; i<items.size();i++){
          sum+=Double.valueOf(items.get(i)) ;
        }
        media=sum/items.size();
        for(int j=0;j<items.size();j++){
            sum2 +=Double.valueOf(Math.pow(items.get(j)-media,2));
        }
        dev=Math.sqrt(sum2/items.size());

        return dev;
    }

     public static void main(String[] args) {
        // 0, 0, 0, 0, 3, 1, 0, 0, 0, 0
        ArrayList<Double> p= new ArrayList<>();
         p.add(0.0);
         p.add(0.0);
         p.add(0.0);
         p.add(0.0);
         p.add(3.0);
         p.add(1.0);
         p.add(0.0);
         p.add(0.0);
         p.add(0.0);
         p.add(0.0);
        double slop= getSlope(p,10);
        System.out.println(slop);

    }

}
