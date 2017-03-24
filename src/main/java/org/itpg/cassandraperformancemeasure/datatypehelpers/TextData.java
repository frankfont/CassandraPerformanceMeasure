/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class TextData extends DataAsString
{
    
    public final static String TYPENAME = "text";
    
    private final String m_seedvalue;
    
    public TextData()
    {
        this(TYPENAME);
    }
    
    public TextData(String seedvalue)
    {
        m_seedvalue = seedvalue;
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
