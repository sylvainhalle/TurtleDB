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
 * Union of two relations. By default, the resulting schema will
 * be that of the first table.
 * @author sylvain
 *
 */
public class Union extends NAryRelation
{   

  @Override
  public void accept(QueryVisitor v) throws VisitorException
  {
    super.acceptNAry(v);
    v.visit(this);
  }

  protected class UnionStreamIterator extends NAryRelationStreamIterator
  {
    public UnionStreamIterator()
    {
      super();
    }
    
    @Override
    protected Tuple internalNext()
    {
      super.initializeIteration();
      return super.incrementSmallestTuple();
    }
  }

  @Override
  public RelationStreamIterator streamIterator()
  {
    return new UnionStreamIterator();
  }

  @Override
  public RelationIterator cacheIterator()
  {
    return new UnionCacheIterator();
  }
  
  protected class UnionCacheIterator extends RelationCacheIterator
  {
    @Override
    protected void getIntermediateResult()
    {
      Table tab = new Table(getSchema());
      for (Relation r : m_relations)
      {
        RelationIterator i = r.cacheIterator();
        while (i.hasNext())
        {
          Tuple t = i.next();
          tab.put(t);
        }
      }
      m_intermediateResult = tab;
    }
  }
}