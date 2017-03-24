/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class BooleanData extends DataAsString
{
    
    public final static String TYPENAME = "float";
    
    private final int m_seednum;
    
    public BooleanData()
    {
        this(1);
    }
    
    public BooleanData(int seedvalue)
    {
        m_seednum = seedvalue;
    }
    
    public Boolean[] getTestValuesAsBooleanArray(int count)
    {
        Boolean[] data = new Boolean[count];
        for(int i=0; i<count; i++)
        {
            data[i] = (((m_seednum + i) % 2) == 0);
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        for(int i=0; i<count; i++)
        {
            data[i] = "" + (((m_seednum + i) % 2) == 0);
        }
        return data;
    }    
}
