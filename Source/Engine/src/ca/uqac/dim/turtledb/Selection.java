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

public class Selection extends UnaryRelation
{
  protected Condition m_condition;
   
  public Selection(Condition c, Relation r)
  {
    super();
    m_condition = c;
    m_relation = r;
  }

  @Override
  public Schema getSchema()
  {
    // The schema of a selection is the same as the schema of
    // its underlying relation
    return m_relation.getSchema();
  }

  public void setCondition(Condition c)
  {
    assert c != null;
    m_condition = c;
  }
  
  @Override
  public void accept(QueryVisitor v) throws EmptyQueryVisitor.VisitorException
  {
    m_relation.accept(v);
    v.visit(this);
  }
  
  protected class SelectionIterator extends UnaryRelationIterator
  { 
    public SelectionIterator()
    {
      super();
    }
    
    protected Tuple internalNext()
    {
      m_nextTuple = null;
      while (m_childIterator.hasNext())
      {
        Tuple t = m_childIterator.next();
        if (m_condition.evaluate(t))
        {
          return t;
        }
      }
      return null;
    }
    
  }

  @Override
  public RelationIterator iterator()
  {
    return new SelectionIterator();
  }

}
