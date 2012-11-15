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
    return smallest_tuple;
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

  @Override
  public void accept(QueryVisitor v) throws VisitorException
  {
    super.acceptNAry(v);
    v.visit(this);
  }

}
