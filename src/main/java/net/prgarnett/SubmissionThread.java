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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;

public class SubmissionThread implements Runnable
{
	private Driver driver;
	private SessionConfig sConf;
	private List<String[]> queries;
    private boolean doStop;
	
	public SubmissionThread(SessionConfig sConf, Driver driver)
	{
		this.sConf = sConf;
		this.driver = driver;
		this.queries = new ArrayList<String[]>();
		this.doStop = false;
	}
	
	public void run()
	{
		synchronized (this)
		{
			try( Session session = driver.session(sConf) )
			{
				while(this.doStop()==false)
				{
					if(!queries.isEmpty())
					{
						this.processLine(queries.get(0)[0], session, queries.get(0)[1]);
						queries.remove(0);
					}
					else
					{
						try
						{
							this.wait();
						}
						catch (InterruptedException e)
						{
							e.printStackTrace();
							System.err.println("SubmissionThread class run method: " + e.getMessage());
						}
					}
				}
				session.close();
				System.out.println("********************************** Thread Finished");
			}
		}
	}

	public synchronized void addQuery(String aQuery, String mode)
	{
		this.queries.add(new String[] {aQuery, mode});
		this.notify();
	}
	
	/**
     * Private method that processes the line in a transaction, requires the
     * session to be past.
     * 
     * @param line
     * @param session 
     */
    private void processLine(String line, Session session, String mode)
    {
        try (Transaction tx = session.beginTransaction())
        {
            
            ResultSummary result = tx.run(line).consume();//process it
        	
            SummaryCounters counters = result.counters();
            if(mode.equals("rels"))
            {
	            if(counters.relationshipsCreated() == 0)
	            {
	            	System.out.println("No relationships created: " + mode + " " + counters.relationshipsCreated());
	            	System.out.println(line);//print the generated cypher code
	            }
            }
            else if(mode.equals("nodes"))
            {
            	if(counters.nodesCreated() == 0)
	            {
	            	System.out.println("No nodes created");
	            	System.out.println(line);//print the generated cypher code
	            }
            }
         
            List<Notification> notes = result.notifications();//get any result notifications
            
            notes.stream().forEach((aNote) -> 
            {
                System.out.println(aNote.description());//print any notes
            });
            
            tx.commit();//commit this transaction
            tx.close();//close the connection
        }
    }

    /**
     * Called once the data runs out.
     */
    public synchronized void setStop()
    {
    	this.notify();//tell it to wake up incase it has gone to sleep
        this.doStop = true;
    }

    /**
     * While the do stop is false, keep going.
     * 
     * @return true or false
     */
    private synchronized boolean doStop()
    {
        return this.doStop;
    }

	public synchronized boolean isWorking()
	{
		return this.queries.isEmpty() == false;
	}
}
