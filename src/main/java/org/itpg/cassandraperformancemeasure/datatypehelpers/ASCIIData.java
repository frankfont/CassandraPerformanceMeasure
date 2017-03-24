/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class ASCIIData extends DataAsString
{
    
    public final static String TYPENAME = "ascii";
    
    private final String m_seedvalue;
    
    public ASCIIData()
    {
        this(TYPENAME);
    }
    
    public ASCIIData(int seedvalue)
    {
        this("#" + seedvalue);
    }
    
    public ASCIIData(String seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public String[] getTestValuesAsASCIIArray(int count)
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
