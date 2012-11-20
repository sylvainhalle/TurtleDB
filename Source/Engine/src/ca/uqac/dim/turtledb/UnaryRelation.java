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

public abstract class UnaryRelation extends Relation
{
  protected Relation m_relation;
  
  public void setRelation(Relation r)
  {
    m_relation = r;
  }

  
  public int tupleCount()
  {
    return m_relation.tupleCount();
  }
  
  protected abstract class UnaryRelationStreamIterator extends RelationStreamIterator
  {
    protected RelationIterator m_childIterator;
    
    public UnaryRelationStreamIterator()
    {
      super();
      m_childIterator = m_relation.streamIterator();
    }
    
    @Override
    public void reset()
    {
      super.reset();
      m_childIterator.reset();
    }
  }
  
  protected abstract class UnaryRelationCacheIterator extends RelationCacheIterator
  {
    protected void getIntermediateResult()
    {
      Table tab_out = new Table(m_relation.getSchema());
      RelationIterator it = m_relation.cacheIterator();
      while (it.hasNext())
      {
        Tuple t = it.next();
        tab_out.put(t);
      }
      m_intermediateResult = tab_out;
    }
  }
}
