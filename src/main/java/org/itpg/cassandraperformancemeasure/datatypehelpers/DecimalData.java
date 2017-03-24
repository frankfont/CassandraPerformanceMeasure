/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.math.BigDecimal;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class DecimalData extends DataAsString
{
    
    public final static String TYPENAME = "decimal";
    
    private final int m_seedvalue;
    
    public DecimalData()
    {
        this(0);
    }
    
    public DecimalData(int seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public BigDecimal[] getTestValuesAsDecimalArray(int count)
    {
        BigDecimal[] data = new BigDecimal[count];
        for(int i=0; i<count; i++)
        {
            data[i] = new BigDecimal(123 + i);// null; //DATASTAX AUTO DETECTION/CODEX FOR DECIMAL NOT WORKING m_seedvalue + i;
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
