package utility;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Locale;

public class StateParser {

    public static State parseCSV(String csvLine) {

        State state = null;
        Integer x=0;
        String[] csvValues = csvLine.split(",");
        if(csvValues[x].isEmpty())
            x=1;
        ArrayList<Integer> sick_number=new ArrayList<>();
        for(int i=4;i<csvValues.length;i++){
            sick_number.add(Integer.parseInt(csvValues[i]));
        }
        //prova
       // System.out.println("Dimensione lista:"+sick_number.size());
        state = new State(
                csvValues[x], // stato
                csvValues [1], //country
//                csvValues[2], // lat
//                csvValues[3], // lon
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
        if(csvValues[x].isEmpty() || csvValues[x].equals(" "))
            x=1;
        ArrayList<Integer> sick_number=new ArrayList<>();
        for(int i=4;i<csvValues.length;i++){
            if(i==4) {
                sick_number.add(Integer.parseInt(csvValues[i]));
            }
            else {
                y=Integer.parseInt(csvValues[i]);
                z=Integer.parseInt(csvValues[(i-1)]);
                if(y<z)
                    r=0;
                else
                    r=y-z;
               // System.out.println("y:"+y+" e z:"+z);
                sick_number.add(r);
            }


        }
        state = new State(
                csvValues[x].toLowerCase(), // stato se c'è solo country metti anche qui country
                csvValues[1].toLowerCase(), // country
//                csvValues[2], // lat
//                csvValues[3], // lon
                sick_number
        );

        return state;
    }

    public static String getWeek(String data) throws ParseException {
        //2020-02-24
        String[] d=data.split("/");

       /* System.out.println("SOno prima del parse "+data);
        DateTimeFormatter df = DateTimeFormatter.ofPattern("MM/dd/YY", Locale.ENGLISH);*/
        LocalDate date = LocalDate.of(Integer.parseInt(d[2]),Integer.parseInt(d[0]),Integer.parseInt(d[1]));
        //System.out.println("Data Locale:"+date);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        String w= "W"+week;
        return w;
    }
    public static ArrayList<String> convertDatetoWeek(ArrayList<String> date){
        ArrayList<String> dates_week=new ArrayList<>();
        for(int i=0;i<date.size();i++) {
            try {
                dates_week.add(getWeek(date.get(i)));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return dates_week;
    }
}
