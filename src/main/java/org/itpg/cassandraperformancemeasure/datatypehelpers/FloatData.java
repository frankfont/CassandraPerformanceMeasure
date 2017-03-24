/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class FloatData extends DataAsString
{
    
    public final static String TYPENAME = "float";
    
    private final float m_seedvalue;
    
    public FloatData()
    {
        this((float) 0.25);
    }
    
    public FloatData(float seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public Float[] getTestValuesAsFloatArray(int count)
    {
        Float[] data = new Float[count];
        float fraction = 0;
        for(int i=0; i<count; i++)
        {
            fraction = (float) (123 + i % 10000) / 1000;
            data[i] = (float) (m_seedvalue + i) + fraction;
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
            fraction = (i % 1000) / 1000;
            data[i] = "" + (m_seedvalue + i + fraction);
        }
        return data;
    }    
}
