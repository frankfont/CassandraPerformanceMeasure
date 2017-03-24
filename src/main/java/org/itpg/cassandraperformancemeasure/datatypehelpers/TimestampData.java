/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.Arrays;
import java.util.Date;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class TimestampData extends DataAsString
{
    
    public final static String TYPENAME = "timestamp";
    
    private final String m_seedvalue;
    
    public TimestampData()
    {
        this("2016-12-10 00:00:00+0000");
    }
    
    public TimestampData(int seednum)
    {
        this("2016-12-17 00:00:00+" + String.format("%04d", seednum % 1000));
    }
    
    public TimestampData(String seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public Date[] getTestValuesAsTimestampArray(int count)
    {
        int offset; 
        String[] parts = m_seedvalue.split("\\+");
        if(parts.length != 2)
        {
            parts = new String[2];
            offset = 0;
        } else {
            offset = Integer.parseInt(parts[1]);
        }
        Date[] data = new Date[count];
        for(int i=0; i<count; i++)
        {
            String suffix = "" + ((offset + i) % 1000);
            data[i] = null; //parts[0] + "+" + suffix;
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        Date[] nativedata = getTestValuesAsTimestampArray(count);
        for(int i=0; i<count; i++)
        {
            data[i] = "" + nativedata[i];
        }
        return data;
    }
}
