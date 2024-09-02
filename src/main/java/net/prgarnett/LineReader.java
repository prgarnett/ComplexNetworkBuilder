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
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class LineReader
{
	private final Driver driver;
	private int cores;
	private SubmissionThreadPool threadpool;
	private String mode;
    
    /**
     * Load up the driver, using the server address, password, and username. Set whether this is an encrypted connection.
     * 
     * @param serveradd
     * @param username 
     * @param password
     * @param encryption
     */
    public LineReader(String serveradd, String username, String password, boolean encryption, String mode)
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
		this.mode = mode;
		this.threadpool = new SubmissionThreadPool(this.driver, this.cores);
		this.threadpool.buildThreadPool();
    }
    
    public LineReader(String serveradd, String username, String password, boolean encryption, int cores, String mode)
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
		this.mode = mode;
		this.threadpool = new SubmissionThreadPool(this.driver, this.cores);
		this.threadpool.buildThreadPool();
    }
    
    /**
     * Give the method the path to the file, it will read any line that does not
     * start with a '#'.
     * 
     * @param path 
     */
    public void ExecuteLines(String path)
    {
        try
        {
        	File targetFile = new File(path);
            
            Scanner lineScanner = new Scanner(targetFile);
            while(lineScanner.hasNext())
            {
                String line = lineScanner.nextLine();
                if(!line.startsWith("#") && !line.isEmpty())
                {
                	this.threadpool.processLineThread(line, this.mode);
                }
            }
            
            lineScanner.close();
            this.driver.close();
        }
        catch (FileNotFoundException e)
        {
            System.err.println(e.getMessage());
        }
    }

	public void setDatabase(String database)
	{
		this.threadpool.setDatabase(database);
	}
//    
//    /**
//     * Private method that processes the line in a transaction, requires the
//     * session to be past.
//     * 
//     * @param line
//     * @param session 
//     */
//    private void processLine(String line, Session session)
//    {
//        try (Transaction tx = session.beginTransaction())
//        {
//            
//            System.out.println(line);
//            ResultSummary result = tx.run(line).consume();
//            List<Notification> notes = result.notifications();
//            
//            notes.stream().forEach((aNote) -> 
//            {
//                System.out.println(aNote.description());
//            });
//            
//            tx.commit();
//            tx.close();
//        }
//    }
}
