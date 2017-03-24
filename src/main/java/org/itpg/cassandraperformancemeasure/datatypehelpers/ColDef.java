/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Column definition
 * @author Frank Font
 */
public class ColDef
{
    String m_fieldname;
    String m_datatypename;
    boolean m_ispk;

    public ColDef(String fieldname, String datatypename)
    {
        this(fieldname, datatypename, false);
    }
    
    public ColDef(String fieldname, String datatypename, boolean ispk)
    {
        m_fieldname = fieldname.trim().toLowerCase();
        m_datatypename = datatypename.trim().toLowerCase();
        m_ispk = ispk;
    }
    
    public String getFieldName()
    {
        return m_fieldname;
    }
    
    public String getTypeName()
    {
        return m_datatypename;
    }
    
    public boolean isPrimaryKey()
    {
        return m_ispk;
    }
}
