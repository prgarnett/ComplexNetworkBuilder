package net.prgarnett;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;

public class SubmissionThreadPool
{
	private final Driver driver;
	private String database;
	private final List<SubmissionThread> subThreads;
	private final int cores;
    private int count;
	
	public SubmissionThreadPool(Driver driver, int cores)
	{
		this.subThreads = new ArrayList<SubmissionThread>();
		this.driver = driver;
		this.cores = cores;
		this.count = 0;
	}
	
	public void buildThreadPool()
	{
		SessionConfig sConf = SessionConfig.builder().withDatabase(database).build();
		
		List<Thread> threads = new ArrayList<>();
		for(int x = 0; x < cores; x++)//make the thread pool
		{
			SubmissionThread sTemp = new SubmissionThread(sConf, driver);
			Thread tTemp = new Thread(sTemp);
			this.subThreads.add(sTemp);
			threads.add(tTemp);
		}
		
		for(Thread aThread: threads)//start the threads
		{
			aThread.start();
		}

		System.out.println("Threads created and started.");
	}
	
	public void processLineThread(String theLine, String mode)
	{
		subThreads.get(count).addQuery(theLine, mode);//send the line to a processing thread
		count++;//increment the count to hit another submission thread
		
		if(count==cores)//we have threads for each core - so once we hit max cores reset
		{
			count=0;//reset and go back to start.
		}
	}
	
	public void setDatabase(String database) {
		this.database = database;
	}

	public List<SubmissionThread> getSubThreads() {
		return subThreads;
	}
}
