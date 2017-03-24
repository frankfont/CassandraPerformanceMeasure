/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class MapData extends DataAsString
{
    private final int MAX_SEEDSIZE = 20;
    public final static String TYPENAME = "map";
    
    private final Map<String,String> m_seedvalue;
    
    public MapData()
    {
        this(new HashMap<String,String>());
    }
    
    public MapData(int seednum)
    {
        int seedlistsize = seednum % MAX_SEEDSIZE;
        HashMap<String,String> seedvalue = new HashMap<>();
        for(int i=0; i<seedlistsize;i++)
        {
            seedvalue.put("sn" + seednum + "." + i,"val"+i);
        }
        m_seedvalue = seedvalue;
    }   
    
    public MapData(Map seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public HashMap<String,String>[] getTestValuesAsMapArray(int count)
    {
        HashMap<String,String>[] data;
        data = new HashMap[count]; 
        HashMap<String,String> dummy1 = new HashMap<>(m_seedvalue);
        dummy1.put("A","avalue");
        dummy1.put("B","bvalue");
        dummy1.put("C","cvalue");
        HashMap<String,String> dummy2 = new HashMap<>(m_seedvalue);
        dummy2.put("A","avalue");
        dummy2.put("B","bvalue");
        dummy2.put("C","cvalue");
        HashMap<String,String> onerow;
        for(int i=0; i<count; i++)
        {
            if(i % 2 == 0)
            {
                onerow = new HashMap<String,String>(dummy1);
            } else {
                onerow = new HashMap<String,String>(dummy2);
            }
            onerow.put("SampleValue","" + i);
            data[i] = onerow;
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        HashMap<String,String>[] nativedata = getTestValuesAsMapArray(count);
        for(int i=0; i<count; i++)
        {
            data[i] = "" + nativedata[i];
        }
        return data;
    }    
}
