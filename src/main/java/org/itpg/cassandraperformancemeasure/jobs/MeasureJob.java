/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure.jobs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Log key measurement metrics
 * All Job classes must extend this one.
 * @author Frank Font
 */
public abstract class MeasureJob extends Thread
{
    public static final String CATEGORY_JOB_TYPE = "JOB_TYPE";
    public static final String CATEGORY_JOB_CREATED = "JOB_CREATED";
    public static final String CATEGORY_JOB_STARTED = "JOB_STARTED";
    public static final String CATEGORY_JOB_FINISHED = "JOB_FINISHED";
    public static final String CATEGORY_INSERT_START = "INSERT_START";
    public static final String CATEGORY_INSERT_DONE = "INSERT_DONE";
    public static final String CATEGORY_SELECT_START = "SELECT_START";
    public static final String CATEGORY_SELECT_DONE = "SELECT_DONE";
    public static final String CATEGORY_UPDATE_START = "UPDATE_START";
    public static final String CATEGORY_UPDATE_DONE = "UPDATE_DONE";
    public static final String CATEGORY_TRUNCATE_DONE = "TRUNCATE_DONE";
    public static final String CATEGORY_COMMENT = "COMMENT";
    public static final String CATEGORY_ERROR = "ERROR";
    public static final String CATEGORY_JOB_ABANDONED = "ABANDONED";
    public static final String CATEGORY_JOB_TOTALTIME = "JOB_TOTALTIME";
    public static final String CATEGORY_STOP_TIMER = "JOB_STOP_TIMER";
    public static final String CATEGORY_RESTART_TIMER = "JOB_RESTART_TIMER";
    
    protected String m_jobname;
    protected File m_outputfile;
    protected DateTimeFormatter m_dtformatter;
    protected PrintWriter m_out=null;
    protected final Instant m_created;
    protected Instant m_started = null;
    protected Instant m_finished = null;
    protected int m_problems = 0;
    protected boolean m_isactive = true;
    protected boolean m_abandoned = false;
    
    protected MeasureJob(String jobname, File outputfile)
    {
        this.m_jobname = jobname;
        this.m_outputfile = outputfile;
        m_dtformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss a");  
        try
        {
            m_out = new PrintWriter(new BufferedWriter(new FileWriter(this.m_outputfile)));
            logProgress(CATEGORY_JOB_CREATED,m_jobname + " -> " + m_outputfile.getPath());
        }
        catch(Exception ex)
        {
            throw new RuntimeException("Failed to create file " + this.m_outputfile + " because " + ex, ex);
        }
        m_created = Instant.now();
    }
    
    /**
     * @return raw number of seconds this job ran
     */
    protected long getRawTotalTime()
    {
        return m_finished.getEpochSecond() - m_started.getEpochSecond();
    }
 
    /**
     * @return raw number of seconds this job ran so far
     */
    public long getRawSecondsSinceStart()
    {
        if(m_started == null)
        {
            return -1;
        }
        Instant now = Instant.now();
        return now.getEpochSecond() - m_started.getEpochSecond();
    }
    
    /**
     * @return ISO8601 format
     */
    protected String getFormattedStartedTime()
    {
        return m_dtformatter.withZone(ZoneId.systemDefault()).format(m_started);
    }
    
    /**
     * @return ISO8601 format
     */
    protected String getFormattedFinishedTime()
    {
        return m_dtformatter.withZone(ZoneId.systemDefault()).format(m_finished);
    }
    
    public String getJobName()
    {
        return m_jobname;
    }
    
    protected final void logProgress(String category, String message)
    {
        try
        {
            if(category.equals(CATEGORY_ERROR))
            {
                m_problems++;
            }
            Instant now = Instant.now();
            String timestamp = m_dtformatter.withZone(ZoneId.systemDefault()).format(Instant.now());   
            m_out.print(timestamp);
            m_out.print("\t");
            m_out.print(now.getEpochSecond());
            m_out.print("\t");
            m_out.print(now.getNano());
            m_out.print("\t");
            m_out.print(m_jobname);
            m_out.print("\t");
            m_out.print(category);
            m_out.print("\t");
            m_out.println(message);
        }
        catch(Exception ex)
        {
            throw new RuntimeException("Failed to log for " + m_jobname 
                    + " with category="+category 
                    + " and message=" + message 
                    + " because " + ex, ex);
        }
    }

    private void createFinishedFlagFile(boolean isokay)
    {
        try
        {
            String suffix = "_DONE" + (isokay ? "_OK" : "_FAILED");
            File folderpath = m_outputfile.getParentFile();
            File donefile = java.nio.file.Paths.get(folderpath.getAbsolutePath(), m_jobname + suffix).toFile();
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(donefile)));
            out.println("Finished the job called " + m_jobname + " with " + m_problems + " logged problems see details in " + m_outputfile.getAbsolutePath());
            out.close();     
            m_isactive=false;
            System.out.println("Job named '" + m_jobname + "' has now finished!");
        }
        catch(Exception ex)
        {
            throw new RuntimeException("Failed to create the finished flag file for " + m_jobname + " because " + ex, ex);
        }
    }
    
    public boolean isActive()
    {
        return m_isactive;
    }
 
    public boolean hasProblems()
    {
        return m_problems > 0;
    }    
    
    public void abandon()
    {
        m_abandoned=true;
        m_problems++;
        logProgress(CATEGORY_JOB_ABANDONED,m_jobname);
        m_out.close();
        createFinishedFlagFile(false);
    }
    
    protected void logStart()
    {
        logProgress(CATEGORY_JOB_STARTED,m_jobname);
        m_started = Instant.now();
        m_isactive = true;
    }
    
    protected void logFinish()
    {
        if(!m_abandoned)
        {
            logProgress(CATEGORY_JOB_FINISHED,m_jobname);
            m_finished = Instant.now();
            if(m_started == null)
            {
                logProgress(CATEGORY_ERROR,"Unable to compute total job time because logStart method was not called!");
            } else {
                long totaltime = m_finished.getEpochSecond() - m_started.getEpochSecond();
                logProgress(CATEGORY_JOB_TOTALTIME,"" + totaltime);
            }
            m_out.close();
            createFinishedFlagFile(m_problems == 0);
        }
    }
    
    /**
     * @return a tab delimited row summarizing the performance
     */
    public abstract String getMeasurementSummary(boolean includeColumnLabels);
    
    @Override
    public String toString()
    {
        return "Measure{" + "m_jobname=" + m_jobname + ", m_outputfile=" + m_outputfile + '}';
    }
    
}
