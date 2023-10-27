package sunny.lab.batch.util;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;


import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadCSVData {
    private static final String CSV_FILE_PATH
            = "..//import.csv";

    public List<Map<String, String>> readAllDataAtOnce(String file)
    {
        List<Map<String, String>> resList = new ArrayList<>();
        Map<String,String> map = null;
        try {

            // Create an object of filereader class with CSV file as a parameter
            FileReader filereader = new FileReader(file);

            // create csvReader object
            CSVReader csvReader = new CSVReaderBuilder(filereader).build();

            // put headers into an array
            String[] header = csvReader.readNext();
            // put the rest of rows into a list
            List<String[]> allData = csvReader.readAll();
            int size = allData.size();
            int len = header.length;
            // print Data
            for (int i=0; i<size; i++) {
                map = new HashMap<>();
                for (int j=0; j<len; j++) {
                    map.put(header[j], allData.get(i)[j]);
                    resList.add(map);

                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return resList;
    }

}