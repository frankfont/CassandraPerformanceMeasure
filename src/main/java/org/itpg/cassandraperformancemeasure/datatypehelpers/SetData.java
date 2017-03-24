/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.HashSet;
import java.util.Set;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class SetData extends DataAsString
{
    private final int MAX_SEEDSIZE = 20;    
    public final static String TYPENAME = "set";
    
    private final Set<String> m_seedvalue;
    
    public SetData()
    {
        this(new HashSet<String>());
    }
    
    public SetData(Set seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public SetData(int seednum)
    {
        int seedlistsize = seednum % MAX_SEEDSIZE;
        HashSet<String> seedvalue = new HashSet<>();
        for(int i=0; i<seedlistsize;i++)
        {
            seedvalue.add("sn" + seednum + "." + i);
        }
        m_seedvalue = seedvalue;
    }
    
    public HashSet<String>[] getTestValuesAsSetArray(int count)
    {
        HashSet<String>[] data;
        data = new HashSet[count]; 
        HashSet<String> dummy1 = new HashSet<>(m_seedvalue);
        dummy1.add("A");
        dummy1.add("C");
        dummy1.add("B");
        HashSet<String> dummy2 = new HashSet<>(m_seedvalue);
        dummy2.add("Y");
        dummy2.add("X");
        dummy2.add("Z");
        HashSet<String> onerow;
        for(int i=0; i<count; i++)
        {
            if(i % 2 == 0)
            {
                onerow = new HashSet<String>(dummy1);
            } else {
                onerow = new HashSet<String>(dummy2);
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
        HashSet<String>[] nativedata = getTestValuesAsSetArray(count);
        for(int i=0; i<count; i++)
        {
            data[i] = "" + nativedata[i];
        }
        return data;
    }
}
