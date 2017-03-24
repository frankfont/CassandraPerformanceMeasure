/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class BigintData extends DataAsString
{
    
    public final static String TYPENAME = "bigint";
    
    private final long m_seedvalue;
    
    public BigintData()
    {
        this(0);
    }
    
    public BigintData(int seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public Long[] getTestValuesAsBigintArray(int count)
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
