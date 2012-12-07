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

import ca.uqac.dim.turtledb.QueryVisitor.VisitorException;

public class Intersection extends NAryRelation
{   

  @Override
  public void accept(QueryVisitor v) throws VisitorException
  {
    super.acceptNAry(v);
    v.visit(this);
  }
  
  protected class IntersectionIterator extends NAryRelationStreamIterator
  {
    /**
     * Implementation of internalNext. This implementation
     * assumes that the relations it draws its tuples from
     * are sorted.
     */
    @Override
    protected Tuple internalNext()
    {
      super.initializeIteration();

      // Iterate through the vector of last tuples and find
      // the smallest one
      Tuple smallest_tuple = null;
      boolean all_equal = false;
      do
      {
        all_equal = allEqual();
        smallest_tuple = super.incrementSmallestTuple();
      } while (!all_equal && smallest_tuple != null);
      // Change tuple's schema to that of first relation
      Relation r = m_relations.get(0);
      Schema sch = r.getSchema();
      Tuple t2 = new Tuple(smallest_tuple);
      t2.setSchema(sch);
      return t2;
    }
    
    /**
     * Determines if the tuples currently held in the tuple
     * vector are all equal
     * @return True if the tuples are all equal and not null,
     *    false otherwise
     */
    protected boolean allEqual()
    {
      int len = m_relations.size();
      Tuple last_elem = null;
      for (int i = 0; i < len; i++)
      {
        Tuple t = m_lastTuple.elementAt(i);
        if (i == 0)
        {
          last_elem = t;
          continue;
        }
        if (t == null)
          return false;
        if (!last_elem.equals(t))
          return false;
      }
      return true;
    }
  }

  @Override
  public RelationStreamIterator streamIterator()
  {
    return new IntersectionIterator();
  }

  @Override
  public RelationIterator cacheIterator()
  {
    return new IntersectionCacheIterator();
  }
  
  protected class IntersectionCacheIterator extends NAryRelationCacheIterator
  {
    @Override
    protected void getIntermediateResult()
    {
      super.getIntermediateResult();
      Table tab = new Table(getSchema());
      Table first_table = m_results.firstElement();
      for (Tuple t : first_table.m_tuples)
      {
        boolean all_in = true;
        for (int i = 1; i < m_results.size(); i++)
        {
          Table tt = m_results.elementAt(i);
          // Set schema of t to that of tt so that it can find it
          Schema sch = tt.getSchema();
          Tuple t2 = new Tuple(t);
          t2.setSchema(sch);
          if (!tt.contains(t2))
          {
            all_in = false;
            break;
          }
        }
        if (all_in)
          tab.put(t);
      }
      m_intermediateResult = tab;
    }
  }

}
