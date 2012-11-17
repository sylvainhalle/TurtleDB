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

/**
 * A Table is a list of tuples. Since the leaves of a relational query
 * tree are always tables, it is the only Relation that actually
 * holds data.
 * <p>
 * Internally, tables are implemented as a <em>sorted</em> linked
 * list of tuples. Sorting is maintained at insertion of each tuple.
 * Consquently, a tuple that is to be modified will be removed, changed,
 * and re-inserted so that the global ordering of tuples is always
 * respected. 
 * @author sylvain
 *
 */
public class Table extends Relation
{
  protected List<Tuple> m_tuples;
  protected Schema m_schema;
  protected int m_cursor;
  protected String m_name;
  
  /**
   * Empty constructor. Should only be called from another constructor.
   */
  /*package*/ Table()
  {
    super();
    m_tuples = new ArrayList<Tuple>();
    m_name = "";
  }
  
  /*package*/ Table(String s)
  {
    this();
    m_name = s;
  }
  
  /**
   * Sets a name for the table. It is preferred to create a table
   * with a name directly through the constructor, as using this method
   * implies changing the table's name into every attribute of every
   * tuple <i>a posteriori</i>.
   * @param name The table's name
   */
  public void setName(String name)
  {
    m_name = name;
    for (Tuple t : m_tuples)
      t.setTable(m_name);
  }

  /**
   * Creates an empty table with given schema
   * @param sch The table's schema
   */
  /*package*/ Table(Schema sch)
  {
    this();
    m_schema = sch;
  }
  
  /**
   * Gives the table's name
   * @return The table's name
   */
  public String getName()
  {
    return m_name;
  }
  
  /**
   * Sets the table's schema
   * @param sch The schema
   */
  protected void setSchema(Schema sch)
  {
	Schema s = new Schema(sch);
	s.setTableName(m_name);
    m_schema = s;
  }

  @Override
  public Schema getSchema()
  {
    return m_schema;
  }
  
  /**
   * Adds a new tuple to the table. It is assumed that
   * the tuple's degree is equal to the schema's degree.
   * An <em>assertion</em> fails otherwise (but no exception
   * is raised). This also affixes the table's name to each
   * attribute, except if the table's name is the empty
   * string.
   * <p>
   * The method put also ensures that the tuple is inserted
   * at the correct location to keep the linked list sorted.
   * @param t The tuple to add
   */
  public void put(Tuple t)
  {
    assert t != null;
    assert t.size() == m_schema.size();
    // Gives the current table's name to all the tuple's attributes
    if (m_name != null && !m_name.isEmpty())
      t.setTable(m_name);
    int index = Collections.binarySearch(m_tuples, t);
    if (index < 0) // We silently ignore tuples that are already present
      m_tuples.add(-index-1, t);
  }
  
  /**
   * Adds a collection of tuples to the table. This is just
   * the repeated application of {@link put} to every tuple in
   * the collection.
   * @param tuples The tuples to add
   */
  public void putAll(Collection<Tuple> tuples)
  {
    for (Tuple t : tuples)
    {
      put(t);
    }
  }
  
  @Override
  public void accept(QueryVisitor v) throws EmptyQueryVisitor.VisitorException
  {
    v.visit(this);
  }
  
  /**
   * Copies the contents of a relation into the current relation.
   * In particular, invoking {@link copy} with a query tree triggers the
   * computation of that query and the storing of the resulting tuples
   * into the current relation. <b>Warning:</b> make sure you reset <tt>r</tt>
   * before calling <tt>copy()</tt>.
   * @param r The relation to copy from
   */
  public void copy(Relation r)
  {
    m_schema = r.getSchema();
    Iterator<Tuple> i = this.iterator();
    while (i.hasNext())
    {
      this.put(i.next());
    }
  }
  
  public int getCardinality()
  {
    return m_tuples.size();
  }
  
  /**
   * Determines if a relation contains a given tuple. Contrarily to the
   * generic implementation of {@link contains}, the method for instances
   * of {@link Table} <em>is</em> efficient, as it simply calls the
   * contains method of the underlying list of tuples. It does not
   * present the side effects (reset of enumeration) that the generic
   * method has.
   * @param tup The tuple to look for
   * @return True if the tuple is present, false otherwise
   */
  @Override
  public boolean contains(Tuple tup)
  {
    if (tup == null)
      return false;
    return m_tuples.contains(tup);
  }
  
  public int tupleCount()
  {
    return m_tuples.size();
  }
  
  @Override
  public final boolean isLeaf()
  {
    return true;
  }

  @Override
  public RelationIterator iterator()
  {
    return new TableIterator();
  }
  
  protected class TableIterator extends RelationIterator
  {
    protected Iterator<Tuple> m_iterator;
    
    public TableIterator()
    {
      m_iterator = m_tuples.iterator();
    }

    @Override
    protected Tuple internalNext()
    {
      if (m_iterator.hasNext())
        return m_iterator.next();
      return null;
    }
    
    public void reset()
    {
      super.reset();
      m_iterator = m_tuples.iterator();
    }
  }

}
