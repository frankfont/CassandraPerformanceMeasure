/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.UUID;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class TimeuuidData extends DataAsString
{
    
    public final static String TYPENAME = "timeuuid";
    
    private final String m_seedvalue;
    
    public TimeuuidData()
    {
        this(UUID.randomUUID().toString());
    }
    
    public TimeuuidData(int seednum)
    {
        this(new UUID(seednum, seednum / 2).toString());
    }
    
    public TimeuuidData(String seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public String[] getTestValuesAsTimeuuidArray(int count)
    {
        String[] data = new String[count];
        for(int i=0; i<count; i++)
        {
            data[i] = null; //UUID.randomUUID().toString();
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        return getTestValuesAsTimeuuidArray(count);
    }
}
