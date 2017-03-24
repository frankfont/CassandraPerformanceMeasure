/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class VarcharData extends DataAsString
{
    
    public final static String TYPENAME = "varchar";
    
    private final String m_seedvalue;
    
    public VarcharData()
    {
        this(TYPENAME);
    }
    
    public VarcharData(int seedvalue)
    {
        m_seedvalue = "S#" + seedvalue;
    }
    
    public VarcharData(String seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public String[] getTestValuesAsVarcharArray(int count)
    {
        String[] data = new String[count];
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
            data[i] = m_seedvalue + i;
        }
        return data;
    }
}
