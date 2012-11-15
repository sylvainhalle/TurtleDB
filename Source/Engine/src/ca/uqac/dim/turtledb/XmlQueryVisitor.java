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
import javax.xml.parsers.*;
import org.w3c.dom.*;

/*package*/ class XmlQueryVisitor extends QueryVisitor
{
  protected Document m_doc;
  protected Stack<Node> m_parts;

  public Document getDocument()
  {
    Node n = m_parts.pop();
    m_doc.appendChild(n);
    return m_doc;
  }

  public XmlQueryVisitor()
  {
    super();
    m_parts = new Stack<Node>();
    m_doc = null;
    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try
    {
      builder = builderFactory.newDocumentBuilder();
      m_doc = builder.newDocument();
    }
    catch (ParserConfigurationException e)
    {
      e.printStackTrace();  
    }
  }

  @Override
  public void visit(Intersection r)
  {
    visitNAry("intersection", r);
  }

  @Override
  public void visit(Union r)
  {
    visitNAry("union", r);
  }

  @Override
  public void visit(Projection r)
  {
    Node n = m_doc.createElement("projection");
    Node schema = createSchemaNode(r.m_schema);
    Node relation = m_parts.pop();
    n.appendChild(schema);
    n.appendChild(relation);
    Node op = m_doc.createElement("operand");
    op.appendChild(n);
    m_parts.push(op);
  }

  @Override
  public void visit(Selection r)
  {
    Node n = m_doc.createElement("selection");
    Node condition = createConditionNode(r.m_condition);
    Node relation = m_parts.pop();
    n.appendChild(condition);
    n.appendChild(relation);
    Node op = m_doc.createElement("operand");
    op.appendChild(n);
    m_parts.push(op);
  }

  @Override
  public void visit(VariableTable r)
  {
    Node n = m_doc.createElement("vartable");
    Node name = m_doc.createElement("name");
    name.setTextContent(r.getName());
    Node site = m_doc.createElement("site");
    site.setTextContent(r.getSite());
    n.appendChild(name);
    n.appendChild(site);
    if (r.m_relation != null)
    {
      Node relation = m_parts.pop();
      n.appendChild(relation);
    }
    Node op = m_doc.createElement("operand");
    op.appendChild(n);
    m_parts.push(op);
  }

  @Override
  public void visit(Table r)
  {
    Node n = m_doc.createElement("table");
    Node schema = createSchemaNode(r.m_schema);
    n.appendChild(schema);
    r.reset();
    while (r.hasNext())
    {
      Tuple t = r.next();
      Node t_node = createTupleNode(t);
      n.appendChild(t_node);
    }
    Node op = m_doc.createElement("operand");
    op.appendChild(n);
    m_parts.push(op);
  }
  
  @Override
  public void visit(Join r) throws VisitorException
  {
    Node n = m_doc.createElement("join");
    Node n_right = m_parts.pop(); // RHS
    Node n_left = m_parts.pop(); // LHS
    Node condition = createConditionNode(r.m_condition);
    n.appendChild(condition);
    n.appendChild(n_left);
    n.appendChild(n_right);
    Node op = m_doc.createElement("operand");
    op.appendChild(n);
    m_parts.push(op);
  }

  @Override
  public void visit(Product r) throws VisitorException
  {
    visitNAry("product", r);
  }
  
  protected void visitNAry(String operator, NAryRelation r)
  {
    Node n = m_doc.createElement(operator);
    for (int i = 0; i < r.m_relations.size(); i++)
    {
      Node n_op = m_parts.pop();
      n.appendChild(n_op);
    }
    Node op = m_doc.createElement("operand");
    op.appendChild(n);
    m_parts.push(op);
  }

  protected Node createSchemaNode(Schema sch)
  {
    Node n = m_doc.createElement("schema");
    for (Attribute a : sch)
    {
      Node attnode = m_doc.createElement("attribute");
      attnode.setTextContent(a.toString());
      n.appendChild(attnode);
    }
    return n;
  }

  protected Node createConditionNode(Condition c)
  {
    XmlConditionVisitor xcv = new XmlConditionVisitor(m_doc);
    c.accept(xcv);
    return xcv.getNode();
  }

  protected Node createTupleNode(Tuple t)
  {
    Node tuple = m_doc.createElement("tuple");
    for (Attribute a : t.keySet())
    {
      String a_name = a.toString();
      String a_val = t.get(a).toString();
      Node atval = m_doc.createElement(a_name);
      atval.setTextContent(a_val);
      tuple.appendChild(atval);
    }
    return tuple;
  }
}
