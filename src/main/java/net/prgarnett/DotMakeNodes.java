package net.prgarnett;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class DotMakeNodes
{
	private String filepath, rankLine, RelsFile;
	private Map<String, String> nodes;
	private List<NodeObject> lines;
	private List<String> ranksList, edgeList;
	private List<LocalDate> rankDates;
	
	public DotMakeNodes(String RelsFile)
	{
		nodes = new HashMap<String, String>();
		lines = new ArrayList<NodeObject>();
		ranksList = new ArrayList<String>();
		edgeList = new ArrayList<String>();
		rankLine = "";
		rankDates = new ArrayList<LocalDate>();
		this.RelsFile = RelsFile;
	}

	public void readTheNodesFile(String NodesFile)
	{
		String type="", firstPart;
		int node_count=0;
		Map<String, String> dataRow, lineData;
		
		try
		{
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(this.getFilepath()+NodesFile));
			
			for (CSVRecord record : records)
            {
				NodeObject aNode = new NodeObject();
				String combLine = "", theLine="", node="";
				dataRow = record.toMap();
				lineData = new HashMap<String, String>();

				String lineKey = "";//, lineValue = "";
				
				for(Map.Entry<String, String> entry: dataRow.entrySet())
				{
					node = "Node_"+node_count;
					aNode.setNode(node);
					if(entry.getKey().equals("type"))//type is a special case so look for it first
					{
						type = entry.getValue();//set the node type
					}
					else if(entry.getKey().startsWith("attribute_label"))//look for start of a attribute label
					{
						String value = entry.getValue().trim();
						value = this.escapeString(value);
						lineKey = value;//log its value
					}
					else if(entry.getKey().startsWith("value"))//look for a value for the attribute
					{
						if(!entry.getValue().isBlank())//if its black we don't want to process and the data isn't add to the map
						{
							String value = entry.getValue().trim();//get the value if not black
							value = this.escapeString(value);
							if(lineKey.equals("date"))
							{
								aNode.setDate(this.formatDate(value));
							}
							else if(lineKey.equals("name")) {
								
								nodes.put(value, node);
								lineData.put(lineKey, value);//put it in the database
							}
//							else 
//							{
//								lineValue = value;
//								lineData.put(lineKey, lineValue);//put it in the database
//							}
						}
						lineKey = "";
					}
				}
				
				for(Map.Entry<String, String> entry : lineData.entrySet())
				{
					combLine = combLine.concat("label=\"" + this.replaceEveryThirdSpace(entry.getValue()) + "\", ");
				}
				
				firstPart = node + " ["+combLine;//+comb_6;
				firstPart = firstPart.substring(0, firstPart.length()-1);
				theLine = firstPart.concat(" shape=\"" + type + "\"];");
				aNode.setTheline(theLine);
				aNode.setUsed(false);
				lines.add(aNode);
				System.out.println(theLine);
				
				node_count++;
            }
		}
		catch(IOException e)
		{
			System.out.println("IO Exception CSVReader class Read Nodes Meth: " + e.getMessage());
		}
	}
	
	public void readTheRelsFile()
	{
		try
		{
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(this.getFilepath()+RelsFile));
			Map<String, String> dataRow;
			
			try (FileWriter writer = new FileWriter(this.getFilepath() + RelsFile.substring(0, RelsFile.length()-4) + "_Nulls.txt")) {
				
				for (CSVRecord record : records)
	            {
					dataRow = record.toMap();
					for(Map.Entry<String, String> entry: dataRow.entrySet())
					{
						if(entry.getKey().equals("match id 1"))//type is a special case so look for it first
						{
							if(this.findNode(nodes.get(entry.getValue().trim()))==null){
								writer.write(entry.getValue().trim()+"\n");
							}
						}
						else if(entry.getKey().equals("match id 2"))
						{
							if(this.findNode(nodes.get(entry.getValue().trim()))==null){
								writer.write(entry.getValue().trim()+"\n");
							}
						}
					}
	            }
				System.out.println("Data has been written to the file.");
	        } catch (IOException e) {
	            System.out.println("An error occurred:");
	            e.printStackTrace();
	        }
			
			records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(this.getFilepath()+RelsFile));
			for (CSVRecord record : records)
            {
				String startNode = "", endNode = "", lineStyle = "";
				dataRow = record.toMap();
				for(Map.Entry<String, String> entry: dataRow.entrySet())
				{
					if(entry.getKey().equals("match id 1"))//type is a special case so look for it first
					{
						System.out.println("Matching this node: " + entry.getValue());
						startNode = nodes.get(entry.getValue().trim());
						this.findNode(startNode).setUsed(true);
					}
					else if(entry.getKey().equals("match id 2"))
					{
						System.out.println("Matching this node: " + entry.getValue());
						endNode = nodes.get(entry.getValue().trim());
						this.findNode(endNode).setUsed(true);
					}
					else if(entry.getKey().equals("value_1"))
					{
						lineStyle = entry.getValue();
					}
				}
				System.out.println(startNode + " -> " + endNode + "(linestyle="+lineStyle+")");
				edgeList.add(startNode + " -> " + endNode + "[linestyle="+lineStyle+"];");//make the edge list
            }
			
		}
		catch(IOException e)
		{
			System.out.println("IO Exception CSVReader class Read Nodes Meth: " + e.getMessage());
		}
	}
	
	public void saveTheFile()
	{

		for(NodeObject aNode : lines)
		{
        	if(aNode.isUsed())
        	{
        		this.addANewDate(aNode.getDate());
        	}
		}
		
		Collections.sort(rankDates);
		
		for(int z = 0; z < rankDates.size(); z++)//make the rank line
		{
			if(z==rankDates.size()-1)
			{	
				rankLine = rankLine.concat("\"" + rankDates.get(z) +"\"; ");
			}
			else
			{			
				rankLine = rankLine.concat("\"" + rankDates.get(z) +"\" -> ");
			}
		}
		
		for(NodeObject aNode : lines)//make the rank list
		{
			if(aNode.isUsed())
        	{
				ranksList.add("{ rank = same; " + aNode.getNode() + "; \"" + aNode.getDate().toString() + "\"; }");
        	}
		}
		
		
		try (FileWriter writer = new FileWriter(this.getFilepath() + RelsFile.substring(0, RelsFile.length()-4) + ".dot")) {
			writer.write("digraph TH2REM {\n"
					+ "\toverlap=false;\n"
					+ "\tsplines=true;\n"
					+ "\tranksep=.75;\n"
					+ "\n"
					+ "\t{\n"
					+ "\t\tnode [shape=plaintext, fontsize=40];\n\n");
            writer.write("\t\t" + rankLine + "\n\t}\n");
            
            for(NodeObject aNode : lines)
			{
            	if(aNode.isUsed())
            	{
            		writer.write("\t" + aNode.getTheline() + "\n");
            	}
			}
            
            for(String entry : ranksList)
			{
            	writer.write("\t" + entry + "\n");
			}
            
            for(String entry : edgeList)
			{
            	writer.write("\t" + entry + "\n");
			}
            
            writer.write("}");
            
            System.out.println("Data has been written to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred:");
            e.printStackTrace();
        }
	}
	
	private String escapeString(String input)
	{
		input = input.trim();
		input = input.replace("\\", "\\\\");
		input = input.replace("\"", "\\\"");
		input = input.replace("'", "\\'");
		
		return input;
	}

	public String getFilepath() {
		return filepath;
	}

	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
	
	private NodeObject findNode(String node) {
		NodeObject temp = null;
		for(NodeObject aNode : lines)
		{
			if(aNode.getNode().equals(node))
			{
				temp = aNode;
				break;
			}
		}
		if(temp==null)
		{
			System.out.println(node);
		}
		return temp;
	}
	
	private LocalDate formatDate(String dateStr)
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
		LocalDate date = null;
        try {
            date = LocalDate.parse(dateStr, formatter);
        } catch (DateTimeParseException e) {
            System.out.println("Invalid format");
            e.printStackTrace();
        }
        return date;
	}
	
	private void addANewDate(LocalDate date)
	{
        boolean found = false;
        for(LocalDate aDate: rankDates)
        {
        	if(aDate.equals(date))
        	{
        		found = true;
        		break;
        	}
        }
        if(!found)
        {
        	rankDates.add(date);
        }            
	}
	
	private String replaceEveryThirdSpace(String aString)
	{
		String replacement = "\n";  // Character to replace the third space with

        StringBuilder builder = new StringBuilder();
        int spaceCount = 0;

        for (int i = 0; i < aString.length(); i++) {
            char currentChar = aString.charAt(i);
            if (currentChar == ' ') {
                spaceCount++;  // Increment space count every time a space is found
                if (spaceCount % 3 == 0) {
                    builder.append(replacement);  // Replace every third space
                    continue;
                }
            }
            builder.append(currentChar);
        }

        String modifiedString = builder.toString();
        return modifiedString;
	}
	
	private static class NodeObject
	{
		private String node, theline;
		private LocalDate date;
		boolean used;
		
		public NodeObject(String node, String theline, LocalDate date, boolean used)
		{
			this.node = node;
			this.theline = theline;
			this.date = date;
			this.used = used;
		}
		
		public NodeObject()
		{
			
		}

		public String getNode() {
			return node;
		}

		public void setNode(String node) {
			this.node = node;
		}

		public String getTheline() {
			return theline;
		}

		public void setTheline(String theline) {
			this.theline = theline;
		}

		public boolean isUsed() {
			return used;
		}

		public void setUsed(boolean used) {
			this.used = used;
		}

		public LocalDate getDate() {
			return date;
		}

		public void setDate(LocalDate date) {
			this.date = date;
		}
	}
}


