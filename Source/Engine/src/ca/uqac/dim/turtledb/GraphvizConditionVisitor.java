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

public class GraphvizConditionVisitor extends ConditionVisitor
{
  protected Stack<String> m_parts;

  public GraphvizConditionVisitor() {
    m_parts = new Stack<String>();
  }

  @Override
  public void visit(LogicalAnd c)
  {
    visitNAry("&land;", c);
  }
  
  protected void visitNAry(String operator, NAryCondition c)
  {
    StringBuilder out = new StringBuilder();
    int len = c.getArity();
    for (int i = 0; i < len; i++)
    {
      String op = m_parts.pop();
      if (i > 0)
        out.append(operator);
      out.append(op);
    }
    m_parts.push(out.toString());
  }

  @Override
  public void visit(LogicalOr c)
  {
    visitNAry("&lor;", c);
  }

  @Override
  public void visit(Equality c)
  {
    StringBuilder out = new StringBuilder();
    out.append(c.m_left.toString()).append("=").append(c.m_right.toString());
    m_parts.push(out.toString());
  }
  
  public String getGraphviz()
  {
    String out = m_parts.peek();
    return out;
  }

}