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

import java.util.Map;
import java.util.Stack;

/*package*/ class GraphvizQueryVisitor extends QueryVisitor
{
  protected StringBuilder m_nodes;
  protected StringBuilder m_connectivity;
  protected Stack<String> m_nodeList;
  protected int m_nodeCounter;
  
  public GraphvizQueryVisitor()
  {
    super();
    m_nodeList = new Stack<String>();
    m_nodes = new StringBuilder();
    m_connectivity = new StringBuilder();
    m_nodeCounter = 0;
  }
  
  public String getGraphviz()
  {
    StringBuilder out = new StringBuilder();
    out.append("graph G\n{\n");
    out.append("  node [shape=plaintext];\n");
    out.append(m_nodes);
    out.append(m_connectivity);
    out.append("}\n");
    return out.toString();
  }
  
  @Override
  public void visit(Selection r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    String m_operand = m_nodeList.pop();
    m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    m_nodes.append("  ").append(newNode).append("[label = <&sigma;<sub>").append(createConditionString(r.m_condition)).append("</sub>>];\n");
    m_nodeList.push(newNode);
  }
  
  @Override
  public void visit(Projection r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    String m_operand = m_nodeList.pop();
    m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    m_nodes.append("  ").append(newNode).append("[label = <&pi;<sub>").append(createSchemaString(r.m_schema)).append("</sub>>];\n");
    m_nodeList.push(newNode);    
  }
  
  @Override
  public void visit(Renaming r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    String m_operand = m_nodeList.pop();
    m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    m_nodes.append("  ").append(newNode).append("[label = <&rho;<sub>").append(createRenamingString(r.m_renamedAttributes)).append("</sub>>];\n");
    m_nodeList.push(newNode);    
  }
  
  @Override
  public void visit(Intersection r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    for (int i = 0; i < r.m_relations.size(); i++)
    {
      String m_operand = m_nodeList.pop();
      m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    } 
    m_nodes.append("  ").append(newNode).append("[label = <&cap;>];\n");
    m_nodeList.push(newNode);    
  }
  
  @Override
  public void visit(Union r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    for (int i = 0; i < r.m_relations.size(); i++)
    {
      String m_operand = m_nodeList.pop();
      m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    } 
    m_nodes.append("  ").append(newNode).append("[label = <&cup;>];\n");
    m_nodeList.push(newNode);       
  }
  
  @Override
  public void visit(Join r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    String m_operand = m_nodeList.pop(); // RHS
    m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    m_operand = m_nodeList.pop(); // LHS
    m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    m_nodes.append("  ").append(newNode).append("[label = <&#x22C8;<sub>").append(createConditionString(r.m_condition)).append("</sub>>];\n");
    m_nodeList.push(newNode);       
  }
  
  @Override
  public void visit(Product r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    for (int i = 0; i < r.m_relations.size(); i++)
    {
      String m_operand = m_nodeList.pop();
      m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    } 
    m_nodes.append("  ").append(newNode).append("[label = <&times;>];\n");
    m_nodeList.push(newNode);       
  }
  
  @Override
  public void visit(Table r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    m_nodes.append("  ").append(newNode).append("[label = <").append(r.getName()).append(">];\n");
    m_nodeList.push(newNode);   
  }
  
  @Override
  public void visit(VariableTable r)
  {
    String newNode = "node" + m_nodeCounter;
    m_nodeCounter++;
    if (r.m_relation != null)
    {
      String m_operand = m_nodeList.pop();
      m_connectivity.append("  ").append(newNode).append(" -- ").append(m_operand).append(";\n");
    }
    m_nodes.append("  ").append(newNode).append("[shape=circle,label = <").append(r.getName()).append(">];\n");
    m_nodeList.push(newNode);       
  }
  
  protected String createConditionString(Condition c)
  {
    GraphvizConditionVisitor gcv = new GraphvizConditionVisitor();
    c.accept(gcv);
    return gcv.getGraphviz();
  }
  
  protected String createSchemaString(Schema sch)
  {
    StringBuilder out = new StringBuilder();
    boolean first = true;
    for (Attribute a : sch)
    {
      out.append(a.toString());
      if (!first)
        out.append(",");
      first = false;
    }
    return out.toString();
  }
  
  protected String createRenamingString(Map<Attribute,Attribute> renaming)
  {
    StringBuilder out = new StringBuilder();
    boolean first = true;
    for (Map.Entry<Attribute,Attribute> e : renaming.entrySet())
    {
      out.append(e.getKey().toString()).append("&rarr;").append(e.getValue().toString());
      if (!first)
        out.append(",");
      first = false;
    }
    return out.toString();
  } 

}
