package utility;

import java.util.ArrayList;

public class StateParser {

    public static State parseCSV(String csvLine) {

        State state = null;
        Integer x=0;
        String[] csvValues = csvLine.split(",");
        if(csvValues[x].isEmpty())
            x=1;
        ArrayList<String> sick_number=new ArrayList<>();
        for(int i=4;i<csvValues.length;i++){
            sick_number.add(csvValues[i]);
        }
        //prova
       // System.out.println("Dimensione lista:"+sick_number.size());
        state = new State(
                csvValues[x], // stato
                csvValues [1], //country
                csvValues[2], // lat
                csvValues[3], // lon
                sick_number
        );

        return state;
    }

    //TODO: non funge perchè c'è uno state contentente la virgola nel nome ->uso nifi
    //Bonaire, Sint Eustatius and Saba in State
    // Korea, South in Country
    public static State parseCSV2(String csvLine) {

        State state = null;
        Integer x=0,y,z,r;
        String[] csvValues = csvLine.split(",");
        if(csvValues[x].isEmpty())
            x=1;
        ArrayList<String> sick_number=new ArrayList<>();
        for(int i=4;i<csvValues.length;i++){
            if(i==4) {
                sick_number.add(csvValues[i]);
            }
            else {
                y=Integer.parseInt(csvValues[i]);
                z=Integer.parseInt(csvValues[(i-1)]);
                r=y-z;
               // System.out.println("y:"+y+" e z:"+z);
                sick_number.add(String.valueOf(r).toString());
                //System.out.println("differenza yz:"+r);
            }
            sick_number.add(csvValues[i]);

        }
        state = new State(
                csvValues[x], // stato se c'è solo country metti anche qui country
                csvValues[1], // country
                csvValues[2], // lat
                csvValues[3], // lon
                sick_number
        );

        return state;
    }
}
