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

public abstract class NAryCondition extends Condition
{
  public List<Condition> m_conditions;
  protected String m_operator;
  
  protected NAryCondition()
  {
    super();
    m_conditions = new LinkedList<Condition>();
  }
  
  public void addCondition(Condition c)
  {
    m_conditions.add(c);
  }
  
  protected void acceptNAry(ConditionVisitor v)
  {
    for (Condition c : m_conditions)
      c.accept(v);
  }
  
  /**
   * Returns the arity of the operator, i.e. the number of
   * operands.
   * @return The arity
   */
  public int getArity()
  {
    return m_conditions.size();
  }
  
  @Override
  public String toString()
  {
    StringBuilder out = new StringBuilder();
    int len = m_conditions.size();
    for (int i = 0; i < len; i++)
    {
      if (i > 0)
        out.append(m_operator);
      Condition c = m_conditions.get(i); 
      out.append(c);
    }
    return out.toString();
  }
}
