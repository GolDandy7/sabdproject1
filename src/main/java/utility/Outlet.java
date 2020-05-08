package utility;

import java.io.Serializable;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.Date;
import java.util.Locale;


public class Outlet implements Serializable {

    private String data;
    private String guariti;
    private String tamponi;

    public Outlet(String data, String guariti, String tamponi) {
        this.data = data;
        this.guariti = guariti;
        this.tamponi = tamponi;

    }

    public String getData() {
        return data;
    }

   public LocalDate getDateTime(){
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        return date;
    }

    public String getWeek() throws ParseException {


        //2020-02-24T18:00:00
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        String w= "W"+week;


        return w;


    }

    public Integer getGuariti() {
        int guar;
        try {
            guar = Integer.parseInt(guariti);
        }
        catch (NumberFormatException e)
        {
            guar = 0;
        }
        return guar;
    }

    public Integer getTamponi() {
        int tamp;
        try {
            tamp = Integer.parseInt(tamponi);
        }
        catch (NumberFormatException e)
        {
            tamp = 0;
        }
        return tamp;
    }



    @Override
    public String toString(){
        try {
            return getWeek() + ", " + getGuariti() + ", " + getTamponi();
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }

}
