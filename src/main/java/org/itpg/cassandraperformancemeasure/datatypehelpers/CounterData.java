/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class CounterData extends DataAsString
{
    
    public final static String TYPENAME = "counter";
    
    private final long m_seedvalue;
    
    public CounterData()
    {
        this(0);
    }
    
    public CounterData(int seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public Long[] getTestValuesAsCounterArray(int count)
    {
        Long[] data = new Long[count];
        for(int i=0; i<count; i++)
        {
            data[i] = m_seedvalue + i;
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        for(int i=0; i<count; i++)
        {
            data[i] = "" + (m_seedvalue + i);
        }
        return data;
    } 
}
