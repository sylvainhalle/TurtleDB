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

import java.util.Stack;
import org.w3c.dom.*;

public class XmlConditionVisitor extends ConditionVisitor
{
  protected Stack<Node> m_parts;
  protected Document m_doc;

  public XmlConditionVisitor(Document doc)
  {
    super();
    m_doc = doc;
  }
  
  @Override
  public void visit(LogicalAnd c)
  {
    Node n = m_doc.createElement("and");
    visitNAry(n, c);
  }
  
  protected void visitNAry(Node operator, NAryCondition c)
  {
    int len = c.getArity();
    for (int i = 0; i < len; i++)
    {
      Node op = m_parts.pop();
      operator.appendChild(op);
    }
    Node out = m_doc.createElement("condition");
    out.appendChild(operator);
    m_parts.push(out);
  }

  @Override
  public void visit(LogicalOr c)
  {
    Node n = m_doc.createElement("or");
    visitNAry(n, c);
  }

  @Override
  public void visit(Equality e)
  {
    Node n = m_doc.createElement("equals");
    Node n_left = createLiteralNode(e.m_left);
    Node n_right = createLiteralNode(e.m_right);
    n.appendChild(n_left);
    n.appendChild(n_right);
    Node n_c = m_doc.createElement("condition");
    n_c.appendChild(n);
    m_parts.push(n_c);
  }
  
  public Node getCondition()
  {
    Node out = m_parts.peek();
    return out;
  }
  
  public Node getNode()
  {
    Node out = m_parts.peek();
    return out;
  }

  protected Node createLiteralNode(Literal l)
  {
    Node n = null;
    if (l instanceof Attribute)
    {
      Attribute a = (Attribute) l;
      n = m_doc.createElement("attribute");
      Node n_n = m_doc.createElement("name");
      n_n.setTextContent(a.getName());
      n.appendChild(n_n);
      Node n_t = m_doc.createElement("table");
      n_t.setTextContent(a.getTableName());
      n.appendChild(n_t);
    }
    if (l instanceof Value)
      n = m_doc.createElement("value");
    n.setTextContent(l.toString());
    return n;
  }

}
