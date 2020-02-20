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

public class LogicalNot extends NAryCondition
{
  public LogicalNot()
  {
    super();
    m_operator = "!";
  }

  @Override
  public boolean evaluate(Tuple t)
  {
    for (Condition c : m_conditions)
    {
      if (!c.evaluate(t))
      {
        return true;
      }
      else
      {
        return false;
      }
    }
    return true;
  }

  public void accept(ConditionVisitor v)
  {
    super.acceptNAry(v);
    v.visit(this);
  }

  @Override
  public String toString()
  {
    if (m_conditions == null || m_conditions.isEmpty())
    {
      return "!";
    }
    return "! (" + m_conditions.get(0) + ")";
  }
}
