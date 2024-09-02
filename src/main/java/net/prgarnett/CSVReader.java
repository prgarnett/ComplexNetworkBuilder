/**
 * This software was developed by Philip Garnett (https://prgarnett.net), University of York, YO10 5GD.
 * 
 * The software was developed during the course of a number of related systems mapping and network analysis projects:
 * 		Working on the Chelsea Manning case.
 * 		MapUKHE (https://MapUKHE.net) - mapping mental health in the UK HE sector.
 * 		ActEarly (https://actearly.org.uk/) - Early life changes to improve health and opportunities for children.
 * 
 * This software is distributed under the GNU General Public License v3.0 - see the attached license file for full details.
 * 
 * Copyright 2021 Philip Garnett
 * 
 */

package net.prgarnett;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class CSVReader implements Runnable
{
	private final Driver driver;
	private int cores;
	private String filepath, database;
	private boolean keepRunning;
	private SubmissionThreadPool threadpool;
	
	public CSVReader(String serveradd, String username, String password, boolean encryption)
	{
		if(encryption)
		{
			Config config = Config.builder().withEncryption().build();

	        this.driver = GraphDatabase.driver( serveradd, AuthTokens.basic( username, password), config);
		}
		else
		{
			this.driver = GraphDatabase.driver( serveradd, AuthTokens.basic( username, password ));
		}
		
		this.cores = Runtime.getRuntime().availableProcessors();

		this.keepRunning = true;
	}
	
	public CSVReader(String serveradd, String username, String password, boolean encryption, int cores)
	{
		if(encryption)
		{
			Config config = Config.builder().withEncryption().build();

	        this.driver = GraphDatabase.driver( serveradd, AuthTokens.basic( username, password), config);
		}
		else
		{
			this.driver = GraphDatabase.driver( serveradd, AuthTokens.basic( username, password ));
		}
		
		this.cores = cores;

		this.keepRunning = true;
	}
	
	public void createThreadPool()
	{
		this.threadpool = new SubmissionThreadPool(this.driver, this.cores);
		this.threadpool.setDatabase(this.database);
		this.threadpool.buildThreadPool();
	}
	
	public void readTheNodesFile()
	{
		String type="", firstPart, theLine;
		Map<String, String> dataRow, lineData;
		
		try
		{
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(this.getFilepath()));
			
			for (CSVRecord record : records)
            {
				String combLine = "";
				dataRow = record.toMap();
				lineData = new HashMap<String, String>();

				String lineKey = "", lineValue = "";
				
				for(Map.Entry<String, String> entry: dataRow.entrySet())
				{
					if(entry.getKey().equals("type"))//type is a special case so look for it first
					{
						type = entry.getValue();//set the node type
					}
					else if(entry.getKey().startsWith("attribute_label"))//look for start of a attribute label
					{
						String value = entry.getValue();
						value = this.escapeString(value);
						lineKey = value;//log its value
					}
					else if(entry.getKey().startsWith("value"))//look for a value for the attribute
					{
						if(!entry.getValue().isBlank())//if its black we don't want to process and the data isn't add to the map
						{
							String value = entry.getValue();//get the value if not black
							value = this.escapeString(value);
							lineValue = value;
							lineData.put(lineKey, lineValue);//put it in the database
						}
						lineKey = "";//wipe the values
						lineValue = "";
					}
				}
				
				for(Map.Entry<String, String> entry : lineData.entrySet())
				{
					combLine = combLine.concat(entry.getKey() + ":"+"\"" + entry.getValue() + "\",");
				}
				
				firstPart = "MERGE(b:"+type+" {"+combLine;//+comb_6;
				firstPart = firstPart.substring(0, firstPart.length()-1);
				theLine = firstPart.concat("})");
				
				this.threadpool.processLineThread(theLine, "nodes");
            }
		}
		catch(IOException e)
		{
			System.out.println("IO Exception CSVReader class Read Nodes Meth: " + e.getMessage());
		}
	}
	
@Deprecated	public void readTheRelsFile()
	{
		String type_1="", type_2 ="", rel_type="",match_key_1="", match_id_2="",match_id_1="", match_value_2="", firstPart, theLine;
		Map<String, String> dataRow, lineData;
		
		try
		{
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(this.getFilepath()));
			
			for (CSVRecord record : records)
            {
				String combLine = "";
				dataRow = record.toMap();
				lineData = new HashMap<String, String>();

				String lineKey = "", lineValue = "";
				
				for(Map.Entry<String, String> entry: dataRow.entrySet())
				{
					if(entry.getKey().equals("node type 1"))//type is a special case so look for it first
					{
						type_1 = entry.getValue();//set the node type
					}
					else if(entry.getKey().equals("node type 2"))
					{
						type_2 = entry.getValue();//set the node type
					}
					else if(entry.getKey().equals("match key 1"))//look for start of a attribute label
					{
						match_key_1 = entry.getValue();
					}
					else if(entry.getKey().equals("match id 1"))//look for start of a attribute label
					{
						match_id_1 = entry.getValue();
					}
					else if(entry.getKey().equals("match key 2"))//look for start of a attribute label
					{
						match_id_2 = entry.getValue();
					}
					else if(entry.getKey().equals("match id 2"))//look for start of a attribute label
					{
						match_value_2 = entry.getValue();
					}
					else if(entry.getKey().equals("rel_type"))
					{
						rel_type = entry.getValue();//set the node type
					}
					else if(entry.getKey().toLowerCase().contains("attribute"))//look for start of a attribute label
					{
						String value = entry.getValue();
						value = this.escapeString(value);
						lineKey = value;//log its value
					}
					else if(entry.getKey().toLowerCase().contains("value"))//look for a value for the attribute
					{
						if(!entry.getValue().isBlank())//if its black we don't want to process and the data isn't add to the map
						{
							String value = entry.getValue();//get the value if not black
							value = this.escapeString(value);
							lineValue = value;
							lineData.put(lineKey, lineValue);//put it in the database
						}
						lineKey = "";//wipe the values
						lineValue = "";
					}
				}
				
				for(Map.Entry<String, String> entry : lineData.entrySet())
				{
					combLine = combLine.concat(entry.getKey() + ":"+"\"" + entry.getValue() + "\",");
				}
				
				
				firstPart = "MATCH (a:" + type_1 + ") WITH a MATCH (b:" + type_2 + ") WHERE a." + match_key_1 + "='" + match_id_1 + "' AND b." + match_id_2 + "='" + match_value_2 + "' MERGE (a)-[r:" + rel_type + "{"+combLine;
				firstPart = firstPart.substring(0, firstPart.length()-1);
				theLine = firstPart.concat("}]->(b) RETURN r");
				
				this.threadpool.processLineThread(theLine, "rels");				
            }
		}
		catch(IOException e)
		{
			System.out.println("IO Exception CSVReader class Read Rels Meth: " + e.getMessage());
		}
	}
	
	public void readTheRelsFileGeneric()
	{		
		try 
		{
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(this.getFilepath()));
			
			for (CSVRecord record : records)
            {
				String node_type_1="", node_type_2="", rel_type="", match_key_1="", match_id_2="",match_id_1="", match_value_2="", firstPart, theLine;
				Map<String, String> dataRow, lineData;
				
				String combLine = "";
				dataRow = record.toMap();
				lineData = new HashMap<String, String>();

				String lineKey = "", lineValue = "";
				
				for(Map.Entry<String, String> entry: dataRow.entrySet())
				{
					if(entry.getKey().equals("node type 1"))//look for start of a attribute label
					{
						node_type_1 = entry.getValue();
					}
					else if(entry.getKey().equals("node type 2"))//look for start of a attribute label
					{
						node_type_2 = entry.getValue();
					}
					else if(entry.getKey().equals("match key 1"))//look for start of a attribute label
					{
						match_key_1 = entry.getValue();
					}
					else if(entry.getKey().equals("match id 1"))//look for start of a attribute label
					{
						match_id_1 = entry.getValue();
					}
					else if(entry.getKey().equals("match key 2"))//look for start of a attribute label
					{
						match_id_2 = entry.getValue();
					}
					else if(entry.getKey().equals("match id 2"))//look for start of a attribute label
					{
						match_value_2 = entry.getValue();
					}
					else if(entry.getKey().equals("rel_type"))
					{
						rel_type = entry.getValue();//set the node type
					}
					else if(entry.getKey().toLowerCase().contains("attribute"))//look for start of a attribute label
					{
						String value = entry.getValue();
						value = this.escapeString(value);
						lineKey = value;//log its value
					}
					else if(entry.getKey().toLowerCase().contains("value"))//look for a value for the attribute
					{
						if(!entry.getValue().isBlank())//if its black we don't want to process and the data isn't add to the map
						{
							String value = entry.getValue();//get the value if not black
							value = this.escapeString(value);
							lineValue = value;
							lineData.put(lineKey, lineValue);//put it in the database
						}
						lineKey = "";//wipe the values
						lineValue = "";
					}
				}
				
				for(Map.Entry<String, String> entry : lineData.entrySet())
				{
					combLine = combLine.concat(entry.getKey() + ":"+"\"" + entry.getValue() + "\",");
				}
				
				firstPart = "MATCH (a:" + node_type_1 + ") WITH a MATCH (b:" + node_type_2 + ") WHERE a." + match_key_1 + "=\"" + match_id_1 + "\" AND b." + match_id_2 + "=\"" + match_value_2 + "\" MERGE (a)-[r:" + rel_type + "{"+combLine;
				firstPart = firstPart.substring(0, firstPart.length()-1);
				theLine = firstPart.concat("}]->(b) RETURN r");
				
				this.threadpool.processLineThread(theLine, "rels");//send it from processing
            }
		}
		catch(IOException e)
		{
			System.err.println("IO Exception CSVReader class Read Rel Gen Meth: " + e.getMessage());
		}
	}	
	
	public void run()
	{
		synchronized (this)
		{
			while(keepRunning)
			{
				System.out.println("Threads processing");
				
				for(SubmissionThread aThread: threadpool.getSubThreads())//stop the threads
				{					
					if(aThread.isWorking())
					{
						System.out.println("Threads " + aThread.hashCode() + " still has work: " + aThread.isWorking());
						this.keepRunning = true;
						break;
					}
					else
					{
						System.out.println("Threads " + aThread.hashCode() + " still has work: " + aThread.isWorking());
						aThread.setStop();
						this.keepRunning = false;
					}
				}
				
				if(!keepRunning)
				{
					System.out.println("Threads stopped closing driver.");
					driver.close();
				}
				else
				{
					try
					{
						this.wait(100);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
						System.err.println("Thread processing CSVReader class: " + e.getMessage());
					}
				}
			}

			System.out.println("End of Data Import.");
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

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getFilepath() {
		return filepath;
	}

	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
}
