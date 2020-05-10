package utility;


public class RegionParser {

    public static Region parseCSVRegion(String line) {
        String[] strings= line.split(",");
        Region r = new Region(
                strings[1], //continente
                strings[0]  //country
        );
        return r;

    }

}

