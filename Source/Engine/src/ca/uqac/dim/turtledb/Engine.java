/*-------------------------------------------------------------------------
    Simple distributed database engine
    Copyright (C) 2012  Sylvain Hallé

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 -------------------------------------------------------------------------*/
package ca.uqac.dim.turtledb;

import java.util.*;
import java.util.Map.Entry;

import org.w3c.dom.Document;

import ca.uqac.dim.turtledb.QueryVisitor.VisitorException;

/**
 * An engine does two things:
 * <ol>
 * <li>It locally hosts relations (or fragments thereof)</li>
 * <li>It receives relational query trees and evaluates them against
 *   the locally-hosted relations, then outputs the resulting
 *   relation</li>
 * </ol>
 * @author sylvain
 *
 */
public class Engine
{
  protected Map<String,Relation> m_tables;
  
  /**
   * The list of query plans that await computation
   */
  protected List<Relation> m_pendingQueries;
  
  /**
   * The total number of tuples received by this site
   */
  protected int m_numTuplesReceived;
  
  /**
   * The site's name
   */
  protected String m_siteName;
  
  /**
   * Instantiates a new database query engine. 
   */
  public Engine(String name)
  {
    super();
    m_tables = new HashMap<String,Relation>();
    m_pendingQueries = new LinkedList<Relation>();
    m_numTuplesReceived = 0;
    m_siteName = name;
  }
  
  /**
   * Stores a new relation within the engine. Normally one would only
   * store instances of {@link Table} here (although any relation
   * can be passed).
   * @param name The relation's name
   * @param r The relation
   */
  public void putRelation(String name, Relation r)
  {
    m_tables.put(name, r);
  }
  
  /**
   * Add a query to process.
   * @param query The query
   */
  public void addQuery(Relation query)
  {
    // Update count of received tuples from the outside world
    m_numTuplesReceived += query.tupleCount();
    if (query.isFragment())
    {
      assert query instanceof VariableTable;
      VariableTable vt = (VariableTable) query;
      if (vt.getSite().compareTo(m_siteName) != 0)
      {
        // The fragment is not destined to this site: therefore it is a query
        // plan. Connect plan's leaves to any local tables...
        TableLinkVisitor tlv = new TableLinkVisitor(m_tables);
        try
        {
          query.accept(tlv);
        }
        catch (EmptyQueryVisitor.VisitorException e)
        {
          e.printStackTrace();
        }
        // Add plan to pending queries        
        m_pendingQueries.add(query);
      }
      else
      {
        // The fragment is destined to this site: iterate over pending queries and
        // check if fragment can be connected to some of their leaves
        for (Relation r : m_pendingQueries)
        {
          TableLinkVisitor tlv = new TableLinkVisitor((VariableTable) query);
          try
          {
            r.accept(tlv);
          }
          catch (EmptyQueryVisitor.VisitorException e)
          {
            e.printStackTrace();
          }
        }
      }
    }
    else
    {
      // Not a fragment; connect plan's leaves to any local tables...
      TableLinkVisitor tlv = new TableLinkVisitor(m_tables);
      try
      {
        query.accept(tlv);
      }
      catch (EmptyQueryVisitor.VisitorException e)
      {
        e.printStackTrace();
      }
      // Add plan to pending queries        
      m_pendingQueries.add(query);
    }
  }
  
  /**
   * Add a set of queries. This is just the repeated application
   * of {@link addQuery} to every element of the collection.
   * @param queries The collection of queries to add
   */
  public void addQuery(Collection<Relation> queries)
  {
    for (Relation q : queries)
      addQuery(q);
  }
  
  /**
   * Process any pending queries
   * @return The results of queries that have been processed 
   */
  public Set<Relation> processPendingQueries()
  {
    Set<Relation> processed = new HashSet<Relation>();
    Iterator<Relation> it = m_pendingQueries.iterator();
    while (it.hasNext())
    {
      Relation pq = it.next();
      ReadyToProcessVisitor rtv = new ReadyToProcessVisitor();
      try
      {
        pq.accept(rtv);
      }
      catch (VisitorException e)
      {
        // This should not happen anyway
        e.printStackTrace();
      }
      if (!rtv.isReady())
        continue;
      // Query is ready
      it.remove(); // Remove from list of pending queries
      // Computes the result and copies it into a new table
      Relation to_add;
      Table result = new Table();
      result.copy(pq);
      to_add = result;
      // If relation is a fragment, affix the fragment's label to the computed result
      if (pq.isFragment())
      {
        VariableTable vt = (VariableTable) pq;
        VariableTable head = new VariableTable(vt.m_name, vt.m_site);
        head.setRelation(result);
        to_add = head;
      }
      // Put that table into list of computed results
      processed.add(to_add);
    }
    return processed;
  }
  
  /**
   * Creates a query plan from a given query
   * @param query The query to execute
   * @return The query plan
   */
  public QueryPlan getQueryPlan(Relation query)
  {
    // TODO
    return null;
  }
  
  /**
   * Locally evaluates a query
   * @param query The query XML document
   * @return A new table containing the results of that query
   */
  public Table evaluate(Relation query)
  {  
    // Update count of received tuples from the outside world
    m_numTuplesReceived += query.tupleCount();
    // Connect leaves to actual tables
    TableLinkVisitor tlv = new TableLinkVisitor(m_tables);
    try
    {
      query.accept(tlv);
    }
    catch (EmptyQueryVisitor.VisitorException e)
    {
      e.printStackTrace();
    }
    // Computes the result and copies it into a new table
    Table out = new Table();
    out.copy(query);
    return out;
  }
  
  /**
   * Locally evaluates a query
   * @param s The query string
   * @return A new table containing the results of that query
   */
  public Table evaluate(String s)
  {
    try
    {
      Relation q = XmlQueryParser.parse(s);
      return evaluate(q);
    }
    catch (XmlQueryParser.ParseException e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  public Table evaluate(Document d)
  {
    try
    {
      Relation q = XmlQueryParser.parse(d);
      return evaluate(q);
    }
    catch (XmlQueryParser.ParseException e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  /**
   * Returns the total number of tuples hosted locally by this
   * database engine. This can be used, in conjunction with
   * {@link getTuplesReceived}, to compute cost metrics.
   * @return The number of tuples
   */
  public int getStorageSize()
  {
    int size = 0;
    for (Entry<String,Relation> e : m_tables.entrySet())
    {
      Relation r = e.getValue();
      size += r.getCardinality();
    }
    return size;
  }
  
  /**
   * Returns the number of tuples this engine received from the outside world.
   * This can be used, in conjunction with
   * {@link getStorageSize}, to compute cost metrics.
   * @return The number of tuples
   */
  public int getTuplesReceived()
  {
    return m_numTuplesReceived;
  }
}
