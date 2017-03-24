/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.itpg.cassandraperformancemeasure.datatypehelpers.ColDef;
import org.itpg.cassandraperformancemeasure.datatypehelpers.TableDef;
import org.itpg.cassandraperformancemeasure.datainteractions.Engine;
import org.itpg.cassandraperformancemeasure.jobs.MeasureJob;
import org.itpg.cassandraperformancemeasure.jobs.SimpleOpsOneTable;

/**
 * Main entry class of the utility
 * Run from the command line without parameters to see usage instructions.
 * @author Frank Font
 */
public class TestRunner
{
    private final static String VERSION_ID = "20161220.3";
    private final static int MAX_THREADS = 1000;
    private final static int MAX_ITERATIONS = 100;
    
    private static String getVersionInfo()
    {
        return "TestRunner v" + VERSION_ID;
    }
    
    private static void showHelp(String msg)
    {
        if(msg != null)
        {
            System.out.println(msg);
            System.out.println("\nHelp information...");
        }
        System.out.println(getVersionInfo());
        System.out.println("Usage as follows...");
        System.out.println("TestRunner ip keyspacename outputfolder testplan sizefactor threadfactor iterationfactor");
        System.out.println("Where...");
        System.out.println("  ip ::= ip address of the cassandra cluster");
        System.out.println("  keyspacename ::= the keyspace name in the cluster");
        System.out.println("  outputfolder ::= where to write the information files");
        System.out.println("  testplan ::= name of the test algorithm to run");
        System.out.println("  sizefactor ::= size of data to work with");
        System.out.println("  threadfactor ::= number of threads to launch");
        System.out.println("  iterationfactor ::= number of iterations for the test plan components");
    }
    
    private static void writeTextFile(File path,String filecontent)
    {
        try
        {
            PrintWriter mywriter = new PrintWriter(new BufferedWriter(new FileWriter(path)));    
            mywriter.println(filecontent);
            mywriter.close();
        }
        catch(Exception ex)
        {
            throw new RuntimeException("Failed writing file at path=" + path + " because " + ex, ex);
        }
    }
    
    public static void main(String[] args)
    {
        
        Cluster cluster;
       
        if(args.length != 7)
        {
            showHelp("You provided " + args.length + " parameters!");
            return;
        }
        String ip;
        String keyspacename;
        File outputpath;
        String testplanname;
        int sizefactor;
        int threadfactor;
        int iterationfactor;
        try
        {
            int argoffset=0;
            ip = args[argoffset++].trim();
            keyspacename = args[argoffset++].trim().toLowerCase();
            outputpath = new File(args[argoffset++]);
            testplanname = args[argoffset++].trim().toLowerCase();
            sizefactor = Integer.parseInt(args[argoffset++]);
            threadfactor = Integer.parseInt(args[argoffset++]);
            iterationfactor = Integer.parseInt(args[argoffset++]);
            
            if(threadfactor < 1)
            {
                throw new RuntimeException("threadfactor cannot be less than 1!");
            }
            if(threadfactor > MAX_THREADS)
            {
                throw new RuntimeException("threadfactor cannot be more than " + MAX_THREADS + "!");
            }
            if(iterationfactor < 1)
            {
                throw new RuntimeException("iterationfactor cannot be less than 1!");
            }
            if(iterationfactor > MAX_ITERATIONS)
            {
                throw new RuntimeException("iterationfactor cannot be more than 1" + MAX_ITERATIONS + "!");
            }
            if(sizefactor < 1)
            {
                throw new RuntimeException("sizefactor cannot be less than 1!");
            }
            int maxsizefactor = ((Integer.MAX_VALUE - 1) / (iterationfactor * threadfactor));
            if(sizefactor > maxsizefactor)
            {
                throw new RuntimeException("sizefactor cannot be less more than " + maxsizefactor + "!  This is computed from all otehr factors and MAXINT.");
            }
        }
        catch(Exception ex)
        {
            String msg = "Failed parsing input arguments because " + ex;
            showHelp(msg);
            throw new RuntimeException(msg, ex);
        }
        try
        {
            //35.160.179.241 <-- source
            //35.165.4.220 <-- destination
            cluster = Cluster.builder().addContactPoint(ip).build();
        }
        catch(Exception ex)
        {
            String msg = "Failed connecting to cluster with ip=" + ip + " because " + ex;
            showHelp(msg);
            throw new RuntimeException(msg, ex);
        }
        
        //debugCQL(cluster);
        //if(cluster != null)
        //    return;
        
        boolean isokay = true;
        File folderpath = null;
        File startupinfopath = null;
        try
        {
            Instant test_start_time = Instant.now();
            DateTimeFormatter foldertimename = DateTimeFormatter.ofPattern("yyyyMMdd_hhmmss"); 
            String ksn_nospaces = keyspacename.replace(" ", "");
            String subfoldername = "results_" + foldertimename.withZone(ZoneId.systemDefault()).format(test_start_time)
                    + "__" + ksn_nospaces + "__s" + sizefactor + "_t" + threadfactor + "_i" + iterationfactor;   
            folderpath = java.nio.file.Paths.get(outputpath.getAbsolutePath(), subfoldername).toFile();
            Files.createDirectories(folderpath.toPath());

            StringBuilder sb = new StringBuilder();
            sb.append("Starting " + getVersionInfo() + "..."); 
            sb.append("\n... ip=" + ip); 
            sb.append("\n... keyspacename=" + keyspacename); 
            sb.append("\n... folderpath=" + folderpath); 
            sb.append("\n... testplanname=" + testplanname); 
            sb.append("\n... sizefactor=" + sizefactor); 
            sb.append("\n... threadfactor=" + threadfactor); 
            sb.append("\n... iterationfactor=" + iterationfactor); 
            sb.append("\n... ClusterName=" + cluster.getClusterName());

            System.out.println(sb.toString());
            
            startupinfopath = java.nio.file.Paths.get(folderpath.getAbsolutePath(), "STARTUP_INFO.txt").toFile();
            writeTextFile(startupinfopath,sb.toString());
            
            //debugCQL(cluster);
            isokay = startTest(test_start_time, cluster, keyspacename, folderpath, testplanname, sizefactor, threadfactor, iterationfactor);
            //dummyTest(cluster);
        }
        catch(Exception ex)
        {
            isokay = false;
            String msg = "!!!!!!!!!!!! Testing failed because " + ex;
            System.err.println(msg);
            cluster.close();
            if(folderpath != null)
            {
                File failedpath = java.nio.file.Paths.get(folderpath.getAbsolutePath(), "FAILED_INFO.txt").toFile();
                writeTextFile(failedpath, msg);
            }
        }
        if(isokay)
        {
            System.out.println("Finished successfully");
        } else {
            System.out.println("Finished with problems!");
        }
    }
    
    private static TableDef getStandardTestTableStructure()
    {
        ArrayList<ColDef> coldefs = new ArrayList<>();
        coldefs.add(new ColDef("fdt_bigint_pk","bigint",true));
        coldefs.add(new ColDef("fdt_bigint","bigint"));
        coldefs.add(new ColDef("fdt_ascii","ascii"));
        coldefs.add(new ColDef("fdt_blob","blob"));
        coldefs.add(new ColDef("fdt_boolean","boolean"));
        coldefs.add(new ColDef("fdt_decimal","decimal"));
        coldefs.add(new ColDef("fdt_float","float"));
        coldefs.add(new ColDef("fdt_double","double"));
        coldefs.add(new ColDef("fdt_inet","inet"));
        coldefs.add(new ColDef("fdt_int","int"));
        coldefs.add(new ColDef("fdt_list","list"));
        coldefs.add(new ColDef("fdt_map","map"));
        coldefs.add(new ColDef("fdt_set","set"));
        coldefs.add(new ColDef("fdt_text","text"));
        coldefs.add(new ColDef("fdt_timestamp","timestamp"));
        coldefs.add(new ColDef("fdt_timeuuid","timeuuid"));
        coldefs.add(new ColDef("fdt_uuid","uuid"));
        coldefs.add(new ColDef("fdt_varchar","varchar"));
        coldefs.add(new ColDef("fdt_varint","varint"));
        TableDef td = new TableDef("standard_structure",coldefs,0); 
        return td;
    }
    
    private static boolean startTest(Instant test_start_time
            , Cluster cluster
            , String keyspacename
            , File folderpath, String testplanname
            , int sizefactor, int threadfactor, int iterationfactor)
    {
        Session session = cluster.connect(keyspacename);//"frank_clear1");
        boolean isokay = true;
        int jobnum;
        int seedvalue = 0;
        ArrayList<MeasureJob> jobs = new ArrayList<>();
        try
        {
            
            //First setup the jobs for the threads
            System.out.println("Creating " + threadfactor + " threads now...");
            if(testplanname.equalsIgnoreCase("truncate_simple") || testplanname.equalsIgnoreCase("simple"))
            {
                String tablename = "test_table";
                if(testplanname.equalsIgnoreCase("truncate_simple"))
                {
                    //First truncate the test table
                    truncateTable(session, tablename);
                }
                for(int i=0; i<threadfactor; i++)
                {
                    jobnum = i+1;
                    String jobname = testplanname + "_job_" + jobnum;
                    String enginename = "engine#" + jobnum;
                    TableDef td = getStandardTestTableStructure();
                    HashMap<String,TableDef> inventory = new HashMap<>();
                    inventory.put("*",td);
                    Engine engine = new Engine(enginename, session, inventory, sizefactor, seedvalue);
                    File jobfile = java.nio.file.Paths.get(folderpath.getAbsolutePath(), jobname + ".txt").toFile();
                    MeasureJob onejob = new SimpleOpsOneTable(jobname,jobfile,engine,sizefactor,iterationfactor,tablename);
                    jobs.add(onejob);
                    System.out.println("Created " + jobname + " with seedvalue=" + seedvalue);
                    seedvalue += (sizefactor * iterationfactor);
                }
            } else {
                throw new RuntimeException("There is no implementation for plan named '" + testplanname + "'!");
            }
            //Now start all the threads
            System.out.println("All threads created, will now start them...");
            for(MeasureJob job: jobs)
            {
                job.start();
            }
            System.out.println("All threads started.  Check the subdirectory for completion flags.");
            HashSet<String> donetracker = new HashSet<>();
            int sleepdelay = 5000;
            String statusmessage = "";
            int dotcount=0;
            while(donetracker.size() < jobs.size())
            {
                for(MeasureJob job: jobs)
                {
                    if(!job.isActive())
                    {
                        donetracker.add(job.getJobName());
                    } else if(job.hasProblems())
                    {
                        //Abandon all the jobs
                        System.out.println("Detected problems in " + job.getJobName() + " .... WILL ABANDON all jobs now!");
                        for(MeasureJob abandonjob: jobs)
                        {
                            abandonjob.abandon();
                            isokay = false;
                        }
                        //We will exit the loop naturally on next pass.
                        sleepdelay = 0;
                    }
                }
                if(donetracker.size() < jobs.size())
                {
                    String newstatusmessage = "Thread status: " + donetracker.size() + " of " +  jobs.size() + " have finished.";
                    if(dotcount > 20 || !newstatusmessage.equals(statusmessage))
                    {
                        //Print this new message.
                        System.out.println(newstatusmessage);
                        statusmessage = newstatusmessage;
                        dotcount=0;
                    } else {
                        //Just a dot to show we are doing something.
                        System.out.print(".");
                        dotcount++;
                    }
                    Thread.sleep(sleepdelay);
                    sleepdelay = 10000; //Larger delay next time.
                }
            }
            System.out.println("Thread status -- all have completed!");
            System.out.println("All details are in " + folderpath.getAbsolutePath());
            cluster.close();
            StringBuilder sb = new StringBuilder();
            int item=0;
            for(MeasureJob job: jobs)
            {
                if(!job.hasProblems())
                {
                    sb.append(job.getMeasurementSummary(item==0));
                    item++;
                } else {
                    isokay = false;
                }
            }
            File startupinfopath = java.nio.file.Paths.get(folderpath.getAbsolutePath(), "FINISHED_SUMMARY_INFO.txt").toFile();
            writeTextFile(startupinfopath,sb.toString());
            return isokay;
        }
        catch(Exception ex)
        {
            for(MeasureJob mj: jobs)
            {
                if(mj.isActive())
                {
                    System.err.println("Abandoning " + mj.getJobName() + " because detected exception in harness --> " + ex);
                    mj.abandon();
                }
            }
            throw new RuntimeException("Failed because " + ex, ex);
        }
    }
    
    private static void truncateTable(Session session, String tablename)
    {
        try
        {
            session.execute("TRUNCATE test_table");
            System.out.println("Truncated table " + tablename);
        }
        catch(Exception ex)
        {
            throw new RuntimeException("Failed to truncate table " + tablename + " because " + ex, ex);
        }
    }
    
    private static void debugCQL(Cluster cluster)
    {
        Session session = cluster.connect("frank_unencrypted1");
        System.out.println("LOOK WE STARTED!");

        // Use select to get the user we just entered
        ResultSet results;
        int rownum=0;
        results = session.execute("SELECT fdt_bigint_pk, fdt_bigint, fdt_ascii FROM test_table");
        for (Row row : results) 
        {
            System.out.format("%d %d %s\n", row.getLong(0), row.getLong(1), row.getString(2));
            rownum++;
        }   
        System.out.println("LOOK WE FINISHED! rows="+rownum);
    }
}
