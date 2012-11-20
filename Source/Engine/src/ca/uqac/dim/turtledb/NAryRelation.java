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

import ca.uqac.dim.turtledb.QueryVisitor.VisitorException;

/**
 * An <i>n</i>-ary relation is an operator &star; that accepts a variable number
 * of operands <i>n</i>, with <i>n</i> &geq; 2, i.e.
 * <i>R</i><sub>1</sub>&star;<i>R</i><sub>2</sub>&star;&hellip;&star;<i>R</i><sub><i>n</i></sub>.
 * Examples of <i>n</i>-ary relations are
 * {@link Union} and {@link Intersection}. Note that an <i>n</i>-ary relation
 * must be associative (hence the {@link Join} is not an <i>n</i>-ary relation).
 * @author sylvain
 *
 */
public abstract class NAryRelation extends Relation
{
  /**
   * The list of relations the operator acts on
   */
  protected List<Relation> m_relations;
  
  protected NAryRelation()
  {
    super();
    m_relations = new LinkedList<Relation>();
  }
  
  @Override
  public Schema getSchema()
  {
    // Sufficient to return schema of first operand
    for (Relation r : m_relations)
      return r.getSchema();
    return null;
  }
  
  public void addOperand(Relation r)
  {
    m_relations.add(r);
  }
  
  /**
   * Returns the arity of the operator, i.e. the number of
   * operands.
   * @return The arity
   */
  public int getArity()
  {
    return m_relations.size();
  }
  
  public int tupleCount()
  {
    int count = 0;
    for (Relation r : m_relations)
      count += r.tupleCount();
    return count;
  }
  
  protected void acceptNAry(QueryVisitor v) throws VisitorException
  {
    for (Relation r : m_relations)
      r.accept(v);
  }
  

  
  protected abstract class NAryRelationStreamIterator extends RelationStreamIterator
  {
    /**
     * Determines if the tuple to output is the first one in the iteration
     */
    protected boolean m_first;
    
    /**
     * Vector containing the last tuple taken from each table
     */
    protected Vector<Tuple> m_lastTuple;
    
    /**
     * Iterators on each relation
     */
    protected Vector<RelationIterator> m_iterators;
    
    public NAryRelationStreamIterator()
    {
      super();
      m_lastTuple = new Vector<Tuple>();
      m_iterators = new Vector<RelationIterator>();
      for (Relation r : m_relations)
      {
        m_iterators.addElement(r.streamIterator());
      }
      reset();
    }
    
    protected void initializeIteration()
    {
      int len = m_iterators.size();
      // Get first tuple of every table and fill m_lastTuple with them
      for (int i = 0; i < len; i++)
      {
        RelationIterator r = m_iterators.get(i);
        if (r.hasNext())
        {
          Tuple t = r.next();
          m_lastTuple.insertElementAt(t, i);
        }
        else
        {
          m_lastTuple.insertElementAt(null, i);
        }
      }
    }

    /**
     * Returns the smallest tuple in the vector of tuples, and
     * increments the relation that produced it
     * @return The smallest tuple
     */
    protected Tuple incrementSmallestTuple()
    {
      int len = m_relations.size();
      Tuple smallest_tuple = null;
      int smallest_index = -1;
      for (int i = len - 1; i >= 0; i--)
      {
        Tuple t = m_lastTuple.get(i);
        if (t == null)
          continue;
        if (smallest_tuple == null || smallest_tuple.compareTo(t) < 0)
        {
          smallest_tuple = t;
          smallest_index = i;
        }
      }
      if (smallest_index == -1)
        return null;
      RelationIterator r = m_iterators.get(smallest_index);
      if (r.hasNext())
      {
        Tuple next_t = r.next();
        m_lastTuple.setElementAt(next_t, smallest_index);
      }
      else
      {
        m_lastTuple.setElementAt(null, smallest_index);
      }
      return smallest_tuple;    
    }
    
    @Override
    public void reset()
    {
      super.reset();
      for (RelationIterator r : m_iterators)
        r.reset();
      m_first = true;
    }
  }
  
  protected class NAryRelationCacheIterator extends RelationCacheIterator
  {
    
    protected Vector<Table> m_results = null;
    
    protected Vector<Tuple> m_lastTuple = new Vector<Tuple>();
    
    protected Vector<Iterator<Tuple>> m_iterators;
    
    public NAryRelationCacheIterator()
    {
      super();
      m_lastTuple = new Vector<Tuple>();
      m_iterators = new Vector<Iterator<Tuple>>();
      for (Relation r : m_relations)
      {
        m_iterators.addElement(r.streamIterator());
      }
      //reset();
    }
    
    @Override
    protected void getIntermediateResult()
    {
      m_results = new Vector<Table>();
      for (Relation r : m_relations)
      {
        Table tab_int = new Table(r.getSchema());
        RelationIterator i = r.cacheIterator();
        while (i.hasNext())
        {
          Tuple t = i.next();
          tab_int.put(t);
        }
        m_results.add(tab_int);
      }
    }
    
    protected void initializeIteration()
    {
      int len = m_results.size();
      // Get first tuple of every table and fill m_lastTuple with them
      for (int i = 0; i < len; i++)
      {
        Table tab = m_results.elementAt(i);
        Iterator<Tuple> r = tab.tupleIterator();
        m_iterators.add(r);
        if (r.hasNext())
        {
          Tuple t = r.next();
          m_lastTuple.insertElementAt(t, i);
        }
        else
        {
          m_lastTuple.insertElementAt(null, i);
        }
      }
    }
    
  }
  
}
