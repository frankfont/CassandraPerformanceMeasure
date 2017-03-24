/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.net.InetAddress;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class INETData extends DataAsString
{
    
    public final static String TYPENAME = "inet";
    
    private final int[] m_seedIPv4;
    
    public INETData()
    {
        this("192.1.1");
    }
    
    public INETData(int seedvalue)
    {
        this("192.1." + seedvalue % 255);
    }
    
    public INETData(String seedvalue)
    {
        String[] seedparts = seedvalue.split("\\.");
        m_seedIPv4 = new int[4];
        int partnum = 0;
        for(String part: seedparts)
        {
            int rawnum;
            if(part == null || part.length() == 0)
            {
                part = "1";
            }
            try
            {
                rawnum = Integer.parseInt(part);
                rawnum = rawnum % 255;
            } catch(Exception ex) {
                //Simply ignore this exception!
                rawnum = 1;
            }
            m_seedIPv4[partnum] = rawnum;
            partnum++;
        }
        for(int i=partnum; i<4; i++)
        {
            m_seedIPv4[i] = 1;    
        }
    }
    
    public InetAddress[] getTestValuesAsINETArray(int count)
    {
        InetAddress[] data = new InetAddress[count];
        String prefixtext = m_seedIPv4[0] + "."
                            + m_seedIPv4[1] + "."
                            + m_seedIPv4[2] + ".";
        for(int i=0; i<count; i++)
        {
            int lastpart = (m_seedIPv4[3] + i) % 255;
            byte[] addr = new byte[4];
            try
            {
                addr[0] = (byte) (m_seedIPv4[0] % 255);
                addr[1] = (byte) (m_seedIPv4[1] % 255);
                addr[2] = (byte) (m_seedIPv4[2] % 255);
                addr[3] = (byte) (m_seedIPv4[3] % 255);
                data[i] = InetAddress.getByAddress(addr);
            }
            catch(Exception ex)
            {
                data[i] = null;
            }
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        String prefixtext = m_seedIPv4[0] + "."
                            + m_seedIPv4[1] + "."
                            + m_seedIPv4[2] + ".";
        for(int i=0; i<count; i++)
        {
            int lastpart = (m_seedIPv4[3] + i) % 255;
            data[i] = prefixtext + lastpart;
        }
        return data;
    }
}
