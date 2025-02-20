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

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class App {

	/***
	 * 
	 * Basic loader. Can either take command line arguments or it can read the config from a text file (see example).
	 * 
	 * Note that in nodes need to be present before relationships are added. 
	 * 
	 * Lines - reads a doc of pre-made cypher code as lines.
	 * CSVNodes - reads a structured CSV file describing nodes (see example file).   
	 * CSVRels - reads a structured CSV file describing relationships between nodes (deprecated).
	 * CSVRelsGen - updated (preferred) reader of structured CSV file describing relationships between nodes (see example file).
	 * Dump - dumps a network as the structured CSV files (somewhat untested still - take care with large networks).
	 * ConfigFile - reads config from text file (see example file) - this is generally the best method of operation and all options are available.
	 * 
	 * --help - prints basic help info
	 * 
	 * @param args
	 */
	public static void main(String... args)
	{		
		if(args[0].equals("-h") || args[0].equals("--help"))
		{
			System.out.println("Usage: mode (ConfigFile/Dump/Lines/CSVNodes/CSVRelsGen), server, username, password, encryption=true/false, database, file");
		}
		else if(args[0].equals("Lines"))//
		{
			LineReader reader = new LineReader(args[1], args[2], args[3], Boolean.parseBoolean(args[4]), args[5]);
			reader.setDatabase(args[5]);
	        reader.ExecuteLines(args[6]);
		}
		else if(args[0].equals("CSVNodes"))
		{
			for(int x = 6; x < args.length; x++)
			{
				CSVReader csvN = new CSVReader(args[1], args[2], args[3], Boolean.parseBoolean(args[4]));
				csvN.setDatabase(args[5]);
				csvN.setFilepath(args[x]);
				csvN.readTheNodesFile();
			}
		}
		else if(args[0].equals("CSVRels"))
		{
			for(int x = 6; x < args.length; x++)
			{
				CSVReader csvR = new CSVReader(args[1], args[2], args[3], Boolean.parseBoolean(args[4]));
				csvR.setDatabase(args[5]);
				csvR.setFilepath(args[x]);
				csvR.readTheRelsFile();
			}
		}
		else if(args[0].equals("CSVRelsGen"))//this is now the favoured relationship adding method 
		{
			for(int x = 6; x < args.length; x++)
			{
				CSVReader csvR = new CSVReader(args[1], args[2], args[3], Boolean.parseBoolean(args[4]));
				csvR.setDatabase(args[5]);
				csvR.setFilepath(args[x]);
				csvR.readTheRelsFileGeneric();
			}
		}
		else if(args[0].equals("DotNodes"))
		{
			DotMakeNodes dotProcessor = new DotMakeNodes(args[3]);
			dotProcessor.setFilepath(args[1]);
			dotProcessor.readTheNodesFile(args[2]);
			dotProcessor.readTheRelsFile();
			dotProcessor.saveTheFile();
		}
		else if(args[0].equals("ParseCSV"))
		{
			ParseExcel parser = new ParseExcel(args[1]);
			parser.readTheExcel();
		} else if (args[0].equals("CheckDataFiles"))
		{
			CheckNodes checkNodes = new CheckNodes(args[1], args[2], args[3]);
			checkNodes.runTheSearch();
		}
		else if(args[0].equals("ConfigFile"))
		{
			try
	        {
				System.out.println("Reading the config file");
	        	File targetFile = new File(args[1]);
	        	
	        	String setting, value, server="", username="", password="", encryption="", filepathnodes="", filespathrels="", database="";
	        	int cores = 1;
	        		            
	            Scanner lineScanner = new Scanner(targetFile);
	            while(lineScanner.hasNext())
	            {
	                String line = lineScanner.nextLine();
		            Scanner tokenScan = new Scanner(line);
		            tokenScan.useDelimiter("=");
	                if(!line.startsWith("#") && !line.isEmpty())
	                {
	                	setting = tokenScan.next();
	                	value = tokenScan.next();
	                	if(setting.equals("server"))
	                	{
	                		server = value;
	        				System.out.println("server set");
	                	}
	                	else if(setting.equals("username"))
	                	{
	                		username = value;
	        				System.out.println("username set");
	                	}
	                	else if(setting.equals("password"))
	                	{
	                		password = value;
	        				System.out.println("password set");
	                	}
	                	else if(setting.equals("encryption"))
	                	{
	                		encryption = value;
	        				System.out.println("encryption set");
	                	}
	                	else if(setting.equals("filepathnodes"))
	                	{
	                		filepathnodes = value;
	        				System.out.println("filepathnodes set");
	                	}
	                	else if(setting.equals("filespathrels"))
	                	{
	                		filespathrels = value;
	        				System.out.println("filespathrels set");
	                	}
	                	else if(setting.equals("database"))
	                	{
	                		database = value;
	        				System.out.println("database set");
	                	}
	                	else if(setting.equals("cores"))
	                	{
	                		cores = Integer.parseInt(value);
	        				System.out.println("cores set");
	                	}
	                }
	                tokenScan.close();
	            }
	            lineScanner.close();
	            
				System.out.println("Config file read, loading data");
	            Scanner nodePathScan = new Scanner(filepathnodes);
	            nodePathScan.useDelimiter(",");
            	while(nodePathScan.hasNext())
    			{
    				CSVReader csvN = new CSVReader(server, username, password, Boolean.parseBoolean(encryption), cores);
    				csvN.setDatabase(database);
    				csvN.createThreadPool();
    				csvN.setFilepath(nodePathScan.next());
    				System.out.println("Reading nodes");
    				csvN.readTheNodesFile();
    				System.out.println("Finished nodes.");
    				Thread stopThread = new Thread(csvN);
    	            stopThread.start();
    	            stopThread.join();//join the thread to enforce thread execution in blocks
    			}
            	nodePathScan.close();

            	System.out.println("Starting rels");
	            Scanner relsPathScan = new Scanner(filespathrels);
	            relsPathScan.useDelimiter(",");
            	while(relsPathScan.hasNext())
    			{
    				CSVReader csvR = new CSVReader(server, username, password, Boolean.parseBoolean(encryption), cores);
    				csvR.setDatabase(database);
    				csvR.createThreadPool();
    				csvR.setFilepath(relsPathScan.next());
    				System.out.println("Reading rels");
    				csvR.readTheRelsFileGeneric();
    				System.out.println("Finished rels.");
    				Thread stopThread = new Thread(csvR);
    	            stopThread.start();
    	            stopThread.join();//join the thread to enforce thread execution in blocks 
    			}
            	relsPathScan.close();

    			System.out.println("Application Finished.");
	        }
			catch(IOException e)
			{
				System.err.println("IO Exception App Class: " + e.getMessage());
			}
			catch (InterruptedException e) 
			{
				e.printStackTrace();
				System.err.println("Interrupted Exception App Class: " + e.getMessage());
			}
		}
	}
}