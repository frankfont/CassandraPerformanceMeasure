/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class ListData extends DataAsString
{
    private final int MAX_SEEDSIZE = 20;
    public final static String TYPENAME = "list";
    
    private final List<String> m_seedvalue;
    
    public ListData()
    {
        this(new ArrayList<String>());
    }
    
    public ListData(int seednum)
    {
        int seedlistsize = seednum % MAX_SEEDSIZE;
        ArrayList<String> seedvalue = new ArrayList<>();
        for(int i=0; i<seedlistsize;i++)
        {
            seedvalue.add("sn" + seednum + "." + i);
        }
        m_seedvalue = seedvalue;
    }
    
    public ListData(List seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public ArrayList<String>[] getTestValuesAsListArray(int count)
    {
        ArrayList<String>[] data;
        data = new ArrayList[count]; 
        ArrayList<String> dummy1 = new ArrayList<>(m_seedvalue);
        dummy1.add("A");
        dummy1.add("B");
        dummy1.add("C");
        ArrayList<String> dummy2 = new ArrayList<>(m_seedvalue);
        dummy2.add("X");
        dummy2.add("Y");
        dummy2.add("Z");
        ArrayList<String> onerow;
        for(int i=0; i<count; i++)
        {
            if(i % 2 == 0)
            {
                onerow = new ArrayList<String>(dummy1);
            } else {
                onerow = new ArrayList<String>(dummy2);
            }
            onerow.add("val#" + i);
            data[i] = onerow;
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        ArrayList<String>[] nativedata = getTestValuesAsListArray(count);
        for(int i=0; i<count; i++)
        {
            data[i] = "" + nativedata[i];
        }
        return data;
    }
}
