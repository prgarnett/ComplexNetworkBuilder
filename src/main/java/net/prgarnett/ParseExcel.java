package net.prgarnett;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVParser;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;

public class ParseExcel {
    private final String filePath;

    public ParseExcel(String filePath) {
        this.filePath = filePath;
    }

    public void readTheExcel() {
        CSVFormat format = CSVFormat.Builder.create()
                .setHeader()  // Use the first record as header
                .setIgnoreHeaderCase(true)
                .setTrim(true) // Trim leading and trailing spaces
                .build();

        try (Reader reader = new FileReader(filePath);
             CSVParser parser = new CSVParser(reader, format); FileWriter writer = new FileWriter("NetData.csv")) {

            // Iterate through CSV records
            for (CSVRecord record : parser) {
                String name = record.get("Project_title").replaceAll("\n", " ");
                String PI = record.get("PI").replaceAll("\n", " ");
                String core_team = record.get("Core_team").replaceAll("\n", " ");
                String start_date = record.get("Start_date").replaceAll("\n", " ");
                String end_date = record.get("End_date").replaceAll("\n", " ");


                String Healthy_livelihoods = record.get("Healthy livelihoods");
                String Healthy_places = record.get("Healthy places");
                String Healthy_learning = record.get("Healthy learning");
                String Physical_activity = record.get("Physical activity & play");
                String Healthy_weight = record.get("Food and healthy weight");
                String Evaluation_theme = record.get("Evaluation theme");

                String[] PIs = PI.split(",");
                for (String lead : PIs) {
                    writer.write("\"Person\",\"Project\",\"name\",\"" + lead.trim() + "\",\"name\",\"" + name + "\",\"Project_lead\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }

                String[] members = core_team.split(",");
                // Iterate through the array of items
                for (String member : members) {
                    writer.write("\"Person\",\"Project\",\"name\",\"" + member.trim() + "\",\"name\",\"" + name + "\",\"Team_member\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }

                if(Healthy_livelihoods.equals("Yes")){
                    writer.write("\"Project\",\"ActEarly\",\"name\",\"" + name + "\",\"name\",\"Healthy Livelihoods\",\"Lead_theme\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }
                if(Healthy_places.equals("Yes")){
                    writer.write("\"Project\",\"ActEarly\",\"name\",\"" + name + "\",\"name\",\"Healthy Places\",\"Lead_theme\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }
                if(Healthy_learning.equals("Yes")){
                    writer.write("\"Project\",\"ActEarly\",\"name\",\"" + name + "\",\"name\",\"Healthy Learning\",\"Lead_theme\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }
                if(Physical_activity.equals("Yes")){
                    writer.write("\"Project\",\"ActEarly\",\"name\",\"" + name + "\",\"name\",\"Physical Activity\",\"Lead_theme\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }
                if(Healthy_weight.equals("Yes")){
                    writer.write("\"Project\",\"ActEarly\",\"name\",\"" + name + "\",\"name\",\"Food and healthy weight\",\"Lead_theme\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }
                if(Evaluation_theme.equals("Yes")){
                    writer.write("\"Project\",\"ActEarly\",\"name\",\"" + name + "\",\"name\",\"UYPRP Evaluation Theme\",\"Lead_theme\",\"date\",\"" + start_date + "\",\"end_date\",\"" + end_date + "\"\n");
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
