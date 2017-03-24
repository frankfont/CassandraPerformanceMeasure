/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure.jobs;

import java.io.File;
import org.itpg.cassandraperformancemeasure.datainteractions.Engine;

/**
 * Run some simple performance tests on one table
 * @author Frank Font
 */
public class SimpleOpsOneTable extends MeasureJob
{
    private final String m_tablename;
    private final int m_sizefactor;
    private final int m_iterationfactor;
    private final Engine m_engine;
    
    public SimpleOpsOneTable(String jobname, File outputfile, Engine engine, int sizefactor, int iterationfactor, String tablename)
    {
        super(jobname, outputfile);
        m_engine = engine;
        m_sizefactor = sizefactor;
        m_iterationfactor = iterationfactor;
        m_tablename = tablename;
        logProgress(CATEGORY_COMMENT,"Initial test data settings\t" + engine.getSettingsInfoAsTextLine());
        logProgress(CATEGORY_COMMENT,"Table for testing:" + tablename);
        logProgress(CATEGORY_COMMENT,"sizefactor:" + sizefactor);
        logProgress(CATEGORY_COMMENT,"iterationfactor:" + iterationfactor);
    }
    
    @Override
    public void run()
    {
        
        String fullclassname = this.getClass().getName();
        int laststart = fullclassname.lastIndexOf(".")+1;
        String shortclassname = fullclassname.substring(laststart);
        
        this.logProgress(MeasureJob.CATEGORY_JOB_TYPE, shortclassname + "\t" + m_sizefactor + "\t" + m_iterationfactor);
        this.logStart();
        try
        {
            
            for (int i = 0; i < m_iterationfactor; i++)
            {
                this.logProgress(MeasureJob.CATEGORY_COMMENT, "Starting iteration#" + i);
                if(i > 0)
                {
                    this.logProgress(MeasureJob.CATEGORY_STOP_TIMER, "Starting creation of more test data...");
                    m_engine.createMoreDataNewPK();
                    this.logProgress(MeasureJob.CATEGORY_RESTART_TIMER, "New test data is ready!");
                }
                this.logProgress(MeasureJob.CATEGORY_INSERT_START, "");
                int insertedcount = m_engine.insertRows(m_tablename);
                this.logProgress(MeasureJob.CATEGORY_INSERT_DONE, "Inserted " + insertedcount);
                this.logProgress(MeasureJob.CATEGORY_SELECT_START, "");
                int selectedcount = m_engine.selectAllRows(m_tablename);
                this.logProgress(MeasureJob.CATEGORY_SELECT_DONE, "Selected "+selectedcount);
                long startpkvalue = m_engine.getMostRecentSeedValue(m_tablename);
                int updatecount = (int)(m_sizefactor/20);
                if(updatecount < 1)
                {
                    updatecount = 1;
                }
                long endpkvalue = startpkvalue + updatecount;
                String fieldtoupdate = "fdt_bigint";
                this.logProgress(MeasureJob.CATEGORY_UPDATE_START, "");
                m_engine.updateRows(m_tablename, startpkvalue, endpkvalue, fieldtoupdate);
                this.logProgress(MeasureJob.CATEGORY_UPDATE_DONE, "Updated " + updatecount + " starting at pk=" + startpkvalue);
                Thread.sleep(50);   //Pause for a moment
            }
            logProgress(CATEGORY_COMMENT,"Final test data settings\t" + m_engine.getSettingsInfoAsTextLine());
        }
        catch (Exception ex)
        {
            String msg = "Failed " + m_jobname + " because " + ex;
            try
            {
                this.logProgress(MeasureJob.CATEGORY_ERROR, msg);    
            }
            catch(Exception subex)
            {
                System.err.println(msg);
                System.err.println("Unable to write to " + m_outputfile + " because " + subex);
            }
            this.abandon();
        }
        this.logFinish();
    }

    @Override
    public String getMeasurementSummary(boolean includeColumnLabels)
    {
        StringBuilder sb = new StringBuilder();
        if(includeColumnLabels)
        {
            sb.append("JobName");
            sb.append("\t");
            sb.append("Problems");
            sb.append("\t");
            sb.append("Started");
            sb.append("\t");
            sb.append("Finished");
            sb.append("\t");
            sb.append("Table");
            sb.append("\t");
            sb.append("Inserted Rows");
            sb.append("\t");
            sb.append("Rows/Iter");
            sb.append("\t");
            sb.append("Total Time");
            sb.append("\t");
            sb.append("Iterations");
            sb.append("\t");
            sb.append("Avg Time per Iter");
            sb.append("\t");
            sb.append("Avg Time per Row");
            sb.append("\n");
        }
        int totalrows = m_sizefactor * m_iterationfactor;
        long rawtotaltime = getRawTotalTime();
        float averagetime = rawtotaltime / m_iterationfactor;
        float averagetimeperrow = averagetime / m_sizefactor;
        sb.append(m_jobname);
        sb.append("\t");
        sb.append(m_problems);
        sb.append("\t");
        sb.append(getFormattedStartedTime());
        sb.append("\t");
        sb.append(getFormattedFinishedTime());
        sb.append("\t");
        sb.append(m_tablename);
        sb.append("\t");
        sb.append(totalrows);
        sb.append("\t");
        sb.append(m_sizefactor);
        sb.append("\t");
        sb.append(rawtotaltime);
        sb.append("\t");
        sb.append(m_iterationfactor);
        sb.append("\t");
        sb.append(averagetime);
        sb.append("\t");
        sb.append(averagetimeperrow);
        sb.append("\n");
        return sb.toString();
    }
}
