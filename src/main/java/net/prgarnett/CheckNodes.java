package net.prgarnett;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class CheckNodes {
    private String nodesFile = "";
    private String edgesFile = "";
    private String missing = "";
    private final Set<String> edges_col_1 = new HashSet<>();
    private final Set<String> edges_col_2 = new HashSet<>();
    private final Set<String> nodes_col = new HashSet<>();

    public CheckNodes(String nodesFile, String edgesFile, String outputfile)
    {
        this.nodesFile = nodesFile;
        this.edgesFile = edgesFile;
        this.missing = outputfile;
    }

    public void runTheSearch()
    {
        CSVFormat format = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setHeader()  // This implies the first record is the header
                .setSkipHeaderRecord(true)  // Automatically skips the header row when parsing
                .build();

        // Read first file
        try (Reader reader = new FileReader(edgesFile); CSVParser parser = new CSVParser(reader, format)) {
            for (CSVRecord record : parser) {
                edges_col_1.add(record.get("match id 1"));  // Adjust column name as needed
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read first file
        try (Reader reader = new FileReader(edgesFile); CSVParser parser = new CSVParser(reader, format)) {
            for (CSVRecord record : parser) {
                edges_col_2.add(record.get("match id 2"));  // Adjust column name as needed
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read second file
        try (Reader reader = new FileReader(nodesFile);
             CSVParser parser = new CSVParser(reader, format)) {
            for (CSVRecord record : parser) {
                nodes_col.add(record.get("value_1"));  // Adjust column name as needed
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Configure CSV format for writing with custom header
        CSVFormat writeFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setHeader("Missing Entries")
                .build();

        // Find and save missing elements
        try (Writer writer = new FileWriter(missing); CSVPrinter printer = new CSVPrinter(writer, writeFormat)) {
            printer.printRecord("***** Missing in Edge Col 1");

            for (String entry : edges_col_1) {
                if (!nodes_col.contains(entry)) {
                    printer.printRecord(entry);
                }
            }

            printer.printRecord("***** Missing in Edge Col 2");

            for (String entry : edges_col_2) {
                if (!nodes_col.contains(entry)) {
                    printer.printRecord(entry);
                }
            }

            printer.printRecord("***** Nodes missing in Edge Cols");

            for (String entry : nodes_col) {
                if (!edges_col_1.contains(entry) && !edges_col_2.contains(entry)) {
                    printer.printRecord(entry);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
