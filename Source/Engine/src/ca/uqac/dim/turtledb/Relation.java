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
 * A relation is implemented as a map from a set of tuples
 * (the relation's <em>key</em>) to a set of tuples
 * @author sylvain
 *
 */
public abstract class Relation implements Iterator<Tuple>
{
  
  protected List<Tuple> m_outputTuples;
  protected Tuple m_nextTuple;
  protected boolean m_internalNextCalled;
  
  /**
   * Method that must be implemented by every non-abstract
   * relation; it returns the next tuple
   * of the enumeration, if any. Methods {@link next} and
   * {@link hasNext} use the return value of {@link internalNext}
   * and additionally remove any duplicate tuples from the output
   * enumeration. Hence a call to {@link next} may result in
   * multiple calls to the relation's {@link internalNext}, if
   * the tuples returned are already part of the result (this is
   * especially true of {@link Projection}s.
   * @return The next tuple, <tt>null</tt> if no such tuple
   * exists
   */
  protected abstract Tuple internalNext();
  
  /**
   * Resets the enumeration of tuples, i.e. starts back at
   * the first tuple of the relation.
   */
  public void reset()
  {
    m_nextTuple = null;
    m_outputTuples.clear();
    m_internalNextCalled = false;
  }
  
  /**
   * Returns the relation's schema
   * @return The schema
   */
  public abstract Schema getSchema();
  
  /**
   * Empty constructor. Should only be called from children's
   * constructors. 
   */
  protected Relation()
  {
    super();
    m_outputTuples = new LinkedList<Tuple>();
    m_internalNextCalled = false;
  }
  
  @Override
  public final boolean hasNext()
  {
    if (!m_internalNextCalled)
    {
      next();
      m_internalNextCalled = true;
    }
    return m_nextTuple != null;
  }
  
  @Override
  public final Tuple next()
  {
    if (!m_internalNextCalled)
    {
      while (true)
      {
        m_nextTuple = internalNext();
        if (m_nextTuple == null)
        {
          break;
        }
        if (!m_outputTuples.contains(m_nextTuple))
        {
          m_outputTuples.add(m_nextTuple);
          break;
        }
      }
    }
    m_internalNextCalled = false;
    return m_nextTuple;
  }
  
  /**
   * A relation's degree is the size of its schema.
   * @return The relation's degree
   */
  public final int getDegree()
  {
    return getSchema().size();
  }
  
  /**
   * Pretty-prints a relation to a string
   */
  @Override
  public String toString()
  {
    StringBuilder out = new StringBuilder();
    Schema sch = this.getSchema();
    for (Attribute s : sch)
    {
      out.append(s).append("\t");
    }
    out.append("\n");
    for (int i = 0; i < sch.size(); i++)
    {
      out.append("--------");
    }
    out.append("\n");
    while (this.hasNext())
    {
      Tuple t = this.next();
      //for (Attribute s : t.keySet())
      for (Attribute s : sch)
      {
    	Value v = t.get(s);
        out.append(v).append("\t");
      }
      out.append("\n");
    }
    return out.toString();
  }
  
  @Override
  public void remove()
  {
    // Not supported at the moment
  }
  
  public abstract void accept(QueryVisitor v) throws QueryVisitor.VisitorException;
  
  /**
   * Computes the cardinality of a relation. Except for instances of
   * {@link Table} (which actually contain concrete tuples), calling
   * this method will trigger the evaluation of the query tree and
   * the enumeration of all tuples. It should be used sparingly.  
   * @return The number of tuples in the relation
   */
  public int getCardinality()
  {
    int size = 0;
    this.reset();
    while (this.hasNext())
    {
      this.next();
      size++;
    }
    return size;
  }
  
  /**
   * Returns the number of actual tuples present in the query.
   * This number is different from the cardinality of the query;
   * it counts the number of tuples that are present in the leaves
   * of the query tree. 
   * @return The tuple count
   */
  public abstract int tupleCount();  
  /**
   * Determines if a relation contains a given tuple. Warning #1: this
   * implementation is inefficient, as it enumerates all tuples until
   * found. Warning #2: using {@link contains} resets any undergoing
   * enumeration made on the relation.
   * @param tup The tuple to look for
   * @return True if the tuple is present, false otherwise
   */
  public boolean contains(Tuple tup)
  {
    this.reset();
    if (tup == null)
      return false;
    assert tup != null;
    while (this.hasNext())
    {
      Tuple t = this.next();
      if (tup.equals(t))
        return true;
    }
    return false;
  }
  
  /**
   * Determines if the query tree is a fragment. This is the
   * case when the tree's root is a VariableTable.
   * @return True if the query tree is a fragment, false otherwise
   */
  public boolean isFragment()
  {
    return false;
  }
  
  /**
   * Determines if a given operator is at the leaf of the
   * query tree. Only instances of {@link Table} and
   * {@link VariableTable} may be leaves.
   * @return
   */
  public boolean isLeaf()
  {
    return false;
  }
}
