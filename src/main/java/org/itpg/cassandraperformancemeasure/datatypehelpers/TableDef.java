/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

import java.util.ArrayList;

/**
 * Information about one table
 * @author frank
 */
public class TableDef
{
    private final String m_tablename;
    private final ArrayList<ColDef> m_coldefs;
    private final int m_pk_col;
    private long m_startdatacreate = 0;
    private long m_enddatacreate = 0;
    private ArrayList<Object[]> m_testdata = null;
    private int m_testdata_seedvalue;
    private ArrayList<Integer> m_seedvalues = new ArrayList<>();
   
    public TableDef(String tablename, ArrayList<ColDef> coldefs)
    {
        this(tablename, coldefs, -1);
    } 
    
    public TableDef(String tablename, ArrayList<ColDef> coldefs, int pk_col)
    {
        m_tablename = tablename;
        m_coldefs = coldefs;
        m_pk_col = pk_col;
    }
    
    public int[] getSeedValues()
    {
        int[] result = new int[m_seedvalues.size()];
        int i=0;
        for(Integer sv: m_seedvalues)
        {
            result[i] = sv.intValue();
            i++;
        }
        return result;
    }
    
    public int getMostRecentSeedValue()
    {
        int lastoffset = m_seedvalues.size()-1;
        return m_seedvalues.get(lastoffset);
    }
    
    public String getSeedValuesAsText(String delimiter)
    {
        int i=0;
        StringBuilder sb = new StringBuilder();
        for(Integer sv: m_seedvalues)
        {
            sb.append(sv);
            i++;
            if(i<m_seedvalues.size())
            {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }
    
    public boolean hasPK()
    {
        return m_pk_col > -1;
    }
    
    public int getPKColumnOffset()
    {
        return m_pk_col;
    }
    
    public String getTablename()
    {
        return m_tablename;
    }
    
    public ArrayList<ColDef> getColumnDefs()
    {
        return m_coldefs;
    }

    public ArrayList<String> getColumnTypeNames()
    {
        ArrayList<String> coltypes = new ArrayList<>();
        for(ColDef coldef: m_coldefs)
        {
            coltypes.add(coldef.getTypeName());
        }
        return coltypes;
    }

    /**
     * @return milliseconds spent creating test data
     */
    public long getTestDataCreationTimeMS()
    {
        return m_enddatacreate - m_startdatacreate;
    }
    
    
    public ArrayList<Object[]> getTestDataAsNativeTypes(int rowcount)
    {
        return createTestDataAsNativeTypes(rowcount, 100);  
    }
    
    public ArrayList<Object[]> getCreatedTestDataAsNativeTypes()
    {
        return m_testdata;
    }

    /**
     * @param tablename
     * @param fieldtoupdate
     * @return text useful for a prepared statement
     */
    public String getUpdateStatementText(String tablename, String fieldtoupdate)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tablename);
        sb.append(" SET ");
        String pkpart = null;
        int i=0;
        for(ColDef coldef: m_coldefs)
        {
            if(!coldef.isPrimaryKey())
            {
                if(fieldtoupdate == null || coldef.getFieldName().equalsIgnoreCase(fieldtoupdate))
                {
                    sb.append(coldef.getFieldName());
                    sb.append("=?");
                    i++;
                    if(fieldtoupdate == null && i<m_coldefs.size())
                    {
                        sb.append(",");
                    }
                }
            } else {
                pkpart = " WHERE " + (coldef.getFieldName()) + "=?";
            }
        }
        if(pkpart != null)
        {
            sb.append(pkpart);
        }
        String sql = sb.toString();
        System.out.println("Update statement: " + sql);
        return sql;
    }
    
    /**
     * @param tablename
     * @return text useful for a prepared statement
     */
    public String getInsertStatementText(String tablename)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(tablename);
        sb.append("(");
        int i=0;
        for(ColDef coldef: m_coldefs)
        {
            sb.append(coldef.getFieldName());
            i++;
            if(i<m_coldefs.size())
            {
                sb.append(",");
            }
        }
        sb.append(") VALUES(");
        i=0;
        for(ColDef coldef: m_coldefs)
        {
            sb.append("?");
            i++;
            if(i<m_coldefs.size())
            {
                sb.append(",");
            }
        }
        sb.append(")");
        String sql = sb.toString();
        System.out.println("Insert statement: " + sql);
        return sql;
    }
    
    /**
     * @param rowcount number of rows to create
     * @param seedvalue where we start the PK value
     * @return rows of test data
     */
    public ArrayList<Object[]> createTestDataAsNativeTypes(int rowcount, int seedvalue)
    {
        m_seedvalues.add(seedvalue);    
        m_startdatacreate = System.currentTimeMillis();
        ArrayList<Object[]> testdata = new ArrayList<>();
        ArrayList<Object[]> allcolumndata = new ArrayList<>();
        //Compute all the data
        for(String typename: this.getColumnTypeNames())
        {
            Object[] nativedata = new Object[rowcount];
            switch (typename)
            {
                case "bigint":
                    nativedata= (new BigintData(seedvalue)).getTestValuesAsBigintArray(rowcount);
                    break;
                case "int":
                    nativedata= (new IntData(seedvalue)).getTestValuesAsIntArray(rowcount);
                    break;
                case "varint":
                    nativedata= (new VarintData(seedvalue)).getTestValuesAsVarintArray(rowcount);
                    break;
                case "float":
                    nativedata= (new FloatData(seedvalue)).getTestValuesAsFloatArray(rowcount);
                    break;
                case "decimal":
                    nativedata= (new DecimalData(seedvalue)).getTestValuesAsDecimalArray(rowcount);
                    break;
                case "double":
                    nativedata= (new DoubleData(seedvalue)).getTestValuesAsDoubleArray(rowcount);
                    break;
                case "counter":
                    nativedata= (new CounterData(seedvalue)).getTestValuesAsCounterArray(rowcount);
                    break;
                case "blob":
                    nativedata= (new BlobData(seedvalue)).getTestValuesAsBlobArray(rowcount);
                    break;
                case "ascii":
                    nativedata= (new ASCIIData(seedvalue)).getTestValuesAsASCIIArray(rowcount);
                    break;
                case "text":
                    nativedata= (new ASCIIData(seedvalue)).getTestValuesAsASCIIArray(rowcount);
                    break;
                case "varchar":
                    nativedata= (new VarcharData(seedvalue)).getTestValuesAsVarcharArray(rowcount);
                    break;
                case "boolean":
                    nativedata= (new BooleanData(seedvalue)).getTestValuesAsBooleanArray(rowcount);
                    break;
                case "inet":
                    nativedata= (new INETData(seedvalue)).getTestValuesAsINETArray(rowcount);
                    break;
                case "list":
                    nativedata= (new ListData(seedvalue)).getTestValuesAsListArray(rowcount);
                    break;
                case "map":
                    nativedata= (new MapData(seedvalue)).getTestValuesAsMapArray(rowcount);
                    break;
                case "set":
                    nativedata= (new SetData(seedvalue)).getTestValuesAsSetArray(rowcount);
                    break;
                case "timestamp":
                    nativedata= (new TimestampData(seedvalue)).getTestValuesAsTimestampArray(rowcount);
                    break;
                case "uuid":
                    nativedata= (new UuidData(seedvalue)).getTestValuesAsUuidArray(rowcount);
                    break;
                case "timeuuid":
                    nativedata= (new TimeuuidData(seedvalue)).getTestValuesAsTimeuuidArray(rowcount);
                    break;
                default:
                    throw new RuntimeException("There is no support for typename='"+typename+"'");
            }
            allcolumndata.add(nativedata);
        }
        //Now pivot the data
        for(int row=0; row<rowcount; row++)
        {
            Object[] onerow = new Object[m_coldefs.size()];
            for(int c=0; c<m_coldefs.size(); c++)
            {
                Object[] nativedata = allcolumndata.get(c);
                onerow[c] = nativedata[row];
            }
            testdata.add(onerow);
        }
        m_enddatacreate = System.currentTimeMillis();
        m_testdata_seedvalue = seedvalue;
        m_testdata = testdata;
        return testdata;
    }
    
    /**
     * Just changes the PK column, reuses all the other existing data.
     * @return same test data as before but with a new PK value
     */
    public ArrayList<Object[]> createTestDataAsNativeTypesJustNewPK()
    {
        if(!hasPK())
        {
            throw new RuntimeException("No PK was declared for table " + m_tablename);
        }
        String coltypename = m_coldefs.get(m_pk_col).getTypeName();
        int rowcount = m_testdata.size();
        int newseedvalue = m_testdata_seedvalue + rowcount;
        m_seedvalues.add(newseedvalue); 
        for(int i=0; i<rowcount; i++)
        {
            Object[] rowdata = m_testdata.get(i);
            if(coltypename.equals("bigint")) 
            {
                m_testdata.get(i)[m_pk_col] = (long) i+newseedvalue;
            } else
            if(coltypename.equals("int")) 
            {
                m_testdata.get(i)[m_pk_col] = (int) i+newseedvalue;
            } else
            if(coltypename.equals("varint")) 
            {
                m_testdata.get(i)[m_pk_col] = (long) i+newseedvalue;
            } else 
            if(coltypename.equals("decimal")) 
            {
                m_testdata.get(i)[m_pk_col] = (long) i+newseedvalue;
            } else {
                throw new RuntimeException("This method does not support PK of type " + coltypename);
            }
        }
        m_testdata_seedvalue = newseedvalue;
        return m_testdata;
    }
}
