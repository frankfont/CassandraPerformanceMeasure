/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.math.BigInteger;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class VarintData extends DataAsString
{
    
    public final static String TYPENAME = "varint";
    
    private final long m_seedvalue;
    
    public VarintData()
    {
        this(0);
    }
    
    public VarintData(int seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public BigInteger[] getTestValuesAsVarintArray(int count)
    {
        BigInteger[] data = new BigInteger[count];
        for(int i=0; i<count; i++)
        {
            data[i] = new BigInteger("" + m_seedvalue + i);
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
