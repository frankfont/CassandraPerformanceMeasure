/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.UUID;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class UuidData extends DataAsString
{
    
    public final static String TYPENAME = "uuid";
    
    private final String m_seedvalue;
    
    public UuidData()
    {
        this(UUID.randomUUID().toString());
    }
    
    public UuidData(int seednum)
    {
        this(new UUID(seednum, seednum / 2).toString());
    }
    
    public UuidData(String seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public UUID[] getTestValuesAsUuidArray(int count)
    {
        UUID[] data = new UUID[count];
        for(int i=0; i<count; i++)
        {
            data[i] = UUID.randomUUID();
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        
        UUID[] realdata = getTestValuesAsUuidArray(count);
        String[] data = new String[count];
        int i=0;
        for(UUID item: realdata)
        {
            data[i] = item.toString();
            i++;
        }
        return data;
    }
}
