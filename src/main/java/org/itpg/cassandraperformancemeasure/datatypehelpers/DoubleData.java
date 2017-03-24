/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class DoubleData extends DataAsString
{
    
    public final static String TYPENAME = "double";
    
    private final double m_seedvalue;
    
    public DoubleData()
    {
        this((double) 0.248);
    }
    
    public DoubleData(double seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    /**
     * Cassandra double=Java float!
     * @param count
     * @return 
     */
    public Double[] getTestValuesAsDoubleArray(int count)
    {
        Double[] data = new Double[count];
        float fraction = 0;
        for(int i=0; i<count; i++)
        {
            fraction = (123 + i % 10000) / 10000;
            data[i] = (double) (m_seedvalue + i) + fraction;
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        int fraction = 0;
        for(int i=0; i<count; i++)
        {
            fraction = (i % 10000) / 10000;
            data[i] = "" + (m_seedvalue + i + fraction);
        }
        return data;
    }     
}
