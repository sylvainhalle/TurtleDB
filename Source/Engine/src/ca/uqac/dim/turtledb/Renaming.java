/*-------------------------------------------------------------------------
    Simple distributed database engine
    Copyright (C) 2012-2020  Sylvain Hallé

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

public class Renaming extends UnaryRelation
{
  protected Map<Attribute,Attribute> m_renamedAttributes;

  public Renaming(Relation rel)
  {
    super();
    m_renamedAttributes = new HashMap<Attribute,Attribute>();
    m_relation = rel;
  }

  public Renaming rename(Attribute from, Attribute to)
  {
    m_renamedAttributes.put(from, to);
    return this;
  }

  @Override
  public Schema getSchema()
  {
    Schema s = new Schema();
    for (Attribute a : m_relation.getSchema())
    {
      if (m_renamedAttributes.containsKey(a))
      {
        s.add(m_renamedAttributes.get(a));
      }
      else
      {
        s.add(a);
      }
    }
    return s;
  }

  /**
   * Computes the renaming of a tuple over a given schema
   * @param t The original tuple
   * @return The renamed tuple
   */
  private Tuple rename(Tuple t)
  {
    if (t == null)
      return null;
    Schema s_orig = m_relation.getSchema();
    Schema s_new = getSchema();
    Value[] parts = new Value[s_orig.size()];
    int i = 0;
    for (Attribute a : s_orig)
    {
      parts[i++] = t.get(a);
    }
    return new Tuple(s_new, parts);
  }

  @Override
  public void accept(QueryVisitor v) throws EmptyQueryVisitor.VisitorException 
  {
    m_relation.accept(v);
    v.visit(this);
  }

  protected class ProjectionStreamIterator extends UnaryRelationStreamIterator
  {
    public ProjectionStreamIterator()
    {
      super();
      m_outputTuples = new LinkedList<Tuple>();
    }

    protected Tuple internalNext()
    {
      Tuple t = m_childIterator.next();
      return rename(t);    
    }
  }

  protected class ProjectionCacheIterator extends UnaryRelationCacheIterator
  {
    public void getIntermediateResult()
    {
      Table tab_out = new Table(getSchema());
      super.getIntermediateResult();
      Iterator<Tuple> it = m_intermediateResult.tupleIterator();
      while (it.hasNext())
      {
        Tuple t = it.next();
        Tuple t2 = rename(t);
        tab_out.put(t2);
      }
      m_intermediateResult = tab_out;
    }
  }

  @Override
  public RelationStreamIterator streamIterator()
  {
    return new ProjectionStreamIterator();
  }

  @Override
  public RelationIterator cacheIterator()
  {
    return new ProjectionCacheIterator();
  }

}
