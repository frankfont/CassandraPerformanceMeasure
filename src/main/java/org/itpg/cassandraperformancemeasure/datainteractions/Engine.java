/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure.datainteractions;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.HashMap;
import org.itpg.cassandraperformancemeasure.datatypehelpers.TableDef;

/**
 * Manage the interactions within one key space
 * @author Frank Font
 */
public final class Engine
{
    private HashMap<String,TableDef> m_schemainfo;
    private final Session m_session;
    private final String m_enginename;
    
    public String getSettingsInfoAsTextLine()
    {
        StringBuilder sb = new StringBuilder();
        int i=0;
        sb.append("keyspace=");
        sb.append(m_session.getLoggedKeyspace());
        sb.append(",");
        sb.append("seedvalues:(");
        for(TableDef td: m_schemainfo.values())
        {
            sb.append(td.getTablename());
            sb.append("=[");
            sb.append(td.getSeedValuesAsText(","));
            sb.append("]");
            i++;
            if(i<m_schemainfo.size())
            {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
    public Engine(String enginename, Session session, HashMap<String,TableDef> schemainfo)
    {
        this(enginename, session, schemainfo, 5, 1);
    }
    
    public Engine(String enginename, Session session, HashMap<String,TableDef> schemainfo, int rowcount)
    {
        this(enginename, session, schemainfo, rowcount, 1);
    }
    
    public Engine(String enginename, Session session, HashMap<String,TableDef> schemainfo, int rowcount, int seedvalue)
    {
        m_enginename = enginename;
        m_session = session;
        m_schemainfo = schemainfo;
        createNewData(rowcount, seedvalue);
    }
    
    /**
     * Create new table data for all our tables.
     * @param rowcount
     * @param seedvalue
     */
    public void createNewData(int rowcount, int seedvalue)
    {
        for(String tablename: m_schemainfo.keySet())
        {
            TableDef tabledef = this.getTableDef(tablename);
            tabledef.createTestDataAsNativeTypes(rowcount, seedvalue);
        }
    }
    
    /**
     * Change the PK of all the table data.
     */
    public void createMoreDataNewPK()
    {
        for(String tablename: m_schemainfo.keySet())
        {
            TableDef tabledef = this.getTableDef(tablename);
            tabledef.createTestDataAsNativeTypesJustNewPK();
        }
    }
    
    public TableDef getTableDef(String tablename)
    {
        TableDef tabledef = m_schemainfo.get(tablename);
        if(tabledef == null)
        {
            tabledef = m_schemainfo.get("*");    
        }
        if(tabledef == null)
        {
            throw new RuntimeException(m_enginename + " did not find TableDef for tablename=" + tablename);
        }
        return tabledef;
    }
    
    /**
     * Delete all the rows of one table.
     * @param tablename 
     */
    public void danger_truncate(String tablename)
    {
        if(m_session.isClosed())
        {
            throw new RuntimeException(m_enginename + ": TRUNCATE " + tablename + " failed because connection in session has been closed!");
        } 
        m_session.execute("TRUNCATE " + tablename);
        System.err.println("TRUNCATED " + tablename);
    }
   
     /**
     * Simply select all the rows of one and iterate through them.
     * @param tablename 
     */
    public int selectAllRows(String tablename)
    {
        return this.selectAllRows(tablename, false);
    }
    
    /**
     * Simply select all the rows of one and iterate through them.
     * @param tablename 
     * @param showColValue 
     */
    public int selectAllRows(String tablename, boolean showColValue)
    {
        try
        {
            if(m_session.isClosed())
            {
                throw new RuntimeException(m_enginename + ": Select from " + tablename + " failed because connection in session has been closed!");
            }        
            System.out.println(m_enginename + ": Starting selection of test data in " + tablename + "...");
            TableDef tabledef = this.getTableDef(tablename);
            int interesting_col = tabledef.hasPK() ? tabledef.getPKColumnOffset() : 0;
            ResultSet results = m_session.execute("SELECT * FROM " + tablename);
            int simplecounter = 0;
            for (Row row : results) 
            {
                simplecounter++;
                if(showColValue)
                {
                    System.out.println("@" + simplecounter + " PK=" + row.getObject(interesting_col));
                }
            }
            System.out.println(m_enginename + ": Selected " + simplecounter + " rows in " + tablename);
            return simplecounter;
        }
        catch(Exception ex)
        {
            System.err.println(m_enginename + ": Failed SELECTION from "+tablename + " because " + ex);
            throw new RuntimeException("Failed insert because " + ex, ex);
        }
    }
    
    /**
     * Insert all the test data into one table
     * @param tablename 
     * @return  
     */
    public int insertRows(String tablename)
    {
        int rownum=0;
        try
        {
            if(m_session.isClosed())
            {
                throw new RuntimeException(m_enginename + ": Insert into " + tablename + " failed because connection in session has been closed!");
            }
            System.out.println(m_enginename + ": Starting test data INSERT into "+tablename+"...");
            TableDef tabledef = this.getTableDef(tablename);

            PreparedStatement ps = m_session.prepare(tabledef.getInsertStatementText(tablename));
            BoundStatement bs = new BoundStatement(ps);
            ArrayList<Object[]> testdata = tabledef.getCreatedTestDataAsNativeTypes();
            for(Object[] row: testdata)
            {
                bs.bind(row);
                m_session.execute(bs);
                rownum++;
                //System.out.println("row#" + rownum + ") " + Arrays.toString(row));
            }
            System.out.println(m_enginename + ": Completed INSERTING " + rownum + " rows into "+tablename+"...");
            return rownum;
        }
        catch(Exception ex)
        {
            System.err.println(m_enginename + ": Failed INSERTING at " + rownum + " into "+tablename + " because " + ex);
            throw new RuntimeException(m_enginename + ": Failed insert because " + ex, ex);
        }
    }    
    
    public int getMostRecentSeedValue(String tablename)
    {
        try
        {
            TableDef tabledef = this.getTableDef(tablename);
            return tabledef.getMostRecentSeedValue();
        }
        catch(Exception ex)
        {
            throw new RuntimeException(m_enginename + ": Failed to get most recent seed value for " + tablename + " because " + ex, ex);
        }
    }
    
    /**
     * Update some rows of the new test data in one table
     * @param tablename 
     * @param startpkvalue 
     * @param endpkvalue 
     * @param fieldtoupdate 
     */
    public void updateRows(String tablename, long startpkvalue, long endpkvalue, String fieldtoupdate)
    {
        long pkvalue=0;
        try
        {
            if(m_session.isClosed())
            {
                throw new RuntimeException(m_enginename + ": Update of " + tablename + " failed because connection in session has been closed!");
            }
            System.out.println(m_enginename + ": Starting test data UPDATE of "+tablename+"...");
            TableDef tabledef = this.getTableDef(tablename);

            PreparedStatement ps = m_session.prepare(tabledef.getUpdateStatementText(tablename,fieldtoupdate));
            BoundStatement bs = new BoundStatement(ps);
            int updatecount=0;
            Object[] row = new Object[2];
            for(pkvalue=startpkvalue; pkvalue<=endpkvalue; pkvalue++)
            {
                row[0] = (long) 100+updatecount;
                row[1] = (long) pkvalue;
                bs.bind(row);
                m_session.execute(bs);
                updatecount++;
            }
            System.out.println(m_enginename + ": Completed UPDATING " + updatecount + " rows in " + tablename + "...");
            //this.selectAllRows(tablename, true);
        }
        catch(Exception ex)
        {
            System.err.println(m_enginename + ": Failed UPDATING at PK=" + pkvalue + " in "+tablename + " because " + ex);
            throw new RuntimeException(m_enginename + ": Failed update because " + ex, ex);
        }
    }    
}
