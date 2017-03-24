/*
 * Database performance measuring utility
 * Copyright (c) 2016 Frank Font of ITPG
 */
package org.itpg.cassandraperformancemeasure.datatypehelpers;

/**
 * Helper for grouping the implementations
 * @author frank
 */
public abstract class DataAsString
{

    public abstract String[] getTestValuesAsTextArray(int count);
      
}
