/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class IntData extends DataAsString
{
    
    public final static String TYPENAME = "int";
    
    private final int m_seedvalue;
    
    public IntData()
    {
        this(0);
    }
    
    public IntData(int seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public Integer[] getTestValuesAsIntArray(int count)
    {
        Integer[] data = new Integer[count];
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
