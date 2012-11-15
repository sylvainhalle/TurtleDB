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

public class Equality extends Condition
{
  protected Literal m_left;
  protected Literal m_right;
  
  public Equality(Literal l, Literal r)
  {
    m_left = l;
    m_right = r;
  }

  @Override
  public boolean evaluate(Tuple t)
  {
    // attribute = attribute
    if (m_left instanceof Attribute && m_right instanceof Attribute)
    {
      Value left = t.get(m_left);
      Value right = t.get(m_right);
      return left.equals(right);
    }
    // attribute = value
    if (m_left instanceof Attribute && m_right instanceof Value)
    {
      Value left = t.get(m_left);
      return left.equals(m_right);
    }
    // value = attribute
    if (m_left instanceof Value && m_right instanceof Attribute)
    {
      return t.get(m_right).equals(m_left);
    }
    // value = value
    if (m_left instanceof Value && m_right instanceof Value)
    {
      return m_left.equals(m_right);
    }
    return false;
  }
  
  public void accept(ConditionVisitor v)
  {
    v.visit(this);
  }

}
