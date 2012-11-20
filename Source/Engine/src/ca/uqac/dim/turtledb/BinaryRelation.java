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

/**
 * A binary relation has exactly two operands.
 * @author sylvain
 */
public abstract class BinaryRelation extends Relation
{
  protected Relation m_left;
  protected Relation m_right;
  
  public void setLeft(Relation r)
  {
    m_left = r;
  }
  
  public void setRight(Relation r)
  {
    m_right = r;
  }
  
  public Relation getLeft()
  {
    return m_left;
  }
  
  public Relation getRight()
  {
    return m_right;
  }

  protected void acceptBinary(QueryVisitor v) throws VisitorException
  {
    m_left.accept(v);
    m_right.accept(v);
  }

  @Override
  public int tupleCount()
  {
    return m_left.tupleCount() + m_right.tupleCount();
  }
  
  protected abstract class BinaryRelationStreamIterator extends RelationStreamIterator
  {
    
  }
  
  protected class BinaryRelationCacheIterator extends RelationCacheIterator
  {
    protected Table m_intermediateLeft;
    protected Table m_intermediateRight;
    
    @Override
    protected void getIntermediateResult()
    {
      RelationIterator it = null;
      it = m_left.cacheIterator();
      m_intermediateLeft = new Table(m_left.getSchema());
      while (it.hasNext())
      {
        Tuple t = it.next();
        m_intermediateLeft.put(t);
      }
      it = m_right.cacheIterator();
      m_intermediateRight = new Table(m_right.getSchema());
      while (it.hasNext())
      {
        Tuple t = it.next();
        m_intermediateRight.put(t);
      }
      m_intermediateRight = new Table(m_right.getSchema());
    }
  }

}
