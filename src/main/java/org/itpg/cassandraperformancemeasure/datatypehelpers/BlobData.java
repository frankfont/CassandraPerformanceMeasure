/*
 * Application to get some metrics of database performance
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import com.datastax.driver.core.utils.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Helper to generate data of this type
 * @author Frank Font
 */
public class BlobData extends DataAsString
{
    
    public final static String TYPENAME = "blob";
    
    private final byte[] m_seedvalue;
    
    static class BlobEx 
    {
        int k;
        ByteBuffer b;
        int getK() {
            return k;
        }
        void setK(int k) {
            this.k = k;
        }
        ByteBuffer getB() {
            return b;
        }
        void setB(ByteBuffer b) {
            this.b = b;
        }
    }

    public BlobData()
    {
        this(1);
    }
    
    public BlobData(int seedvalue)
    {
        int smallnum = seedvalue % 255;
        m_seedvalue = new byte[1];
        m_seedvalue[0] = (byte) smallnum;
    }
    
    public BlobData(byte[] seedvalue)
    {
        m_seedvalue = seedvalue;
    }
    
    public ByteBuffer[] getTestValuesAsBlobArray(int count)
    {
        //int bytearraylen = m_seedvalue.length + 5;
        ByteBuffer[] data = new ByteBuffer[count];
        for(int rowi=0; rowi<count; rowi++)
        {
            /*
            onerow = new byte[bytearraylen];
            for(int i=0; i<m_seedvalue.length; i++)
            {
                onerow[i] = m_seedvalue[i];
            }
            for(int i=m_seedvalue.length; i<bytearraylen; i++)
            {
                onerow[i] = (byte) ((rowi + i) % 255);
            }
            */
            ByteBuffer onerow;
            onerow = ByteBuffer.allocate(10);
            onerow.put( (byte) ((5 + m_seedvalue.length) % 255));
            onerow.put( (byte) ((4 + rowi) % 255));
            onerow.put( (byte) ((3 + rowi) % 255));
            onerow.put( (byte) ((2 + rowi) % 255));
            onerow.put( (byte) ((1 + rowi) % 255));
            onerow.put( (byte) ((22 + rowi) % 255));
            onerow.put( (byte) ((33 + rowi) % 255));
            //onerow = null;
            data[rowi] = onerow;
        }
        return data;
    }
    
    @Override
    public String[] getTestValuesAsTextArray(int count)
    {
        String[] data = new String[count];
        Object[] nativedata = getTestValuesAsBlobArray(count);
        for(int i=0; i<count; i++)
        {
            data[i] = Arrays.toString((byte[])nativedata[i]);
        }
        return data;
    }  
}
