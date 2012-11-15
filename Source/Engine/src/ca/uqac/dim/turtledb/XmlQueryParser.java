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

import java.io.IOException;
import java.util.*;

import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

/**
 * Facilities to build relational queries from an XML representation.
 * The XmlQueryParser works in pair with the {@link XmlQueryFormatter}; more
 * precisely, for any {@link Relation} <tt>r</tt>, we should have that
 * <code>
 * XmlQueryParser.parse(XmlQueryFormatter.toXmlDocument(r)) == r
 * </code> 
 * @author sylvain
 *
 */
public class XmlQueryParser
{
  /**
   * Builds a query from a string
   * @param s A string containing an XML representation of the query
   * @return The query
   */
  public static Relation parse(String s) throws XmlQueryParser.ParseException
  {
    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try
    {
      builder = builderFactory.newDocumentBuilder();
    }
    catch (ParserConfigurationException e)
    {
      e.printStackTrace();  
    }
    try
    {
      Document document = builder.parse(s);
      return parse(document);
    }
    catch (SAXException e)
    {
      e.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  /**
   * Builds a query from a DOM document
   * @param s A DOM document containing an XML representation of the query
   * @return The query
   */
  public static Relation parse(Document doc) throws XmlQueryParser.ParseException
  {
    return parse(doc.getDocumentElement());
  }
  
  /**
   * Parse an operand. XML syntax:
   * <pre>
   * &lt;operand&gt;
   *   &lt;x&gt;&hellip;&lt;/x&gt;
   * &lt;/operand&gt;
   * </pre>
   * where <tt>x</tt> is either <tt>intersection</tt>, <tt>union</tt>,
   * <tt>selection</tt>, <tt>projection</tt> or <tt><tt>table</tt>.   
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parse(Node e) throws XmlQueryParser.ParseException
  {
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      String name = n.getLocalName().toLowerCase();
      if (name.compareTo("selection") == 0)
      {
        return parseSelection(n);
      }
      else if (name.compareTo("projection") == 0)
      {
        return parseProjection(n);
      }
      else if (name.compareTo("union") == 0)
      {
        return parseUnion(n);
      }
      else if (name.compareTo("intersection") == 0)
      {
        return parseIntersection(n);
      }
      else if (name.compareTo("join") == 0)
      {
        return parseJoin(n);
      }
      else if (name.compareTo("product") == 0)
      {
        return parseProduct(n);
      }
      else if (name.compareTo("vartable") == 0)
      {
        return parseVariableTable(n);
      }
      else if (name.compareTo("table") == 0)
      {
        return parseTable(n);
      }
    }
    // If we get here, we did not recognize any operand we know
    throw new XmlQueryParser.ParseException("Unrecognized operand");
  }
  
  /**
   * Parse a selection. XML syntax:
   * <pre>
   * &lt;projection&gt;
   *   &lt;condition&gt;&hellip;&lt;/condition&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   * &lt;/projection&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parseSelection(Node e) throws XmlQueryParser.ParseException
  {
    Condition c = null;
    Relation r = null;
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("condition") == 0)
        c = parseCondition(n);
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        r = parse(n);
      }
    }
    if (c == null)
      throw new XmlQueryParser.ParseException("Missing condition in selection");
    if (r == null)
      throw new XmlQueryParser.ParseException("Missing operand in selection");
    Selection sel = new Selection(c, r);
    return sel;
  }
  
  /**
   * Parse a projection. XML syntax:
   * <pre>
   * &lt;projection&gt;
   *   &lt;schema&gt;&hellip;&lt;/schema&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   * &lt;/projection&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parseProjection(Node e) throws XmlQueryParser.ParseException
  {
    Schema s = null;
    Relation r = null;
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("schema") == 0)
        s = parseSchema(n);
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        r = parse(n);
      }
    }
    if (s == null)
      throw new XmlQueryParser.ParseException("Missing schema in projection");
    if (r == null)
      throw new XmlQueryParser.ParseException("Missing operand in projection");
    Projection pro = new Projection(s, r);
    return pro;
  }
  
  /**
   * Parse a union. XML syntax:
   * <pre>
   * &lt;union&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &hellip;
   * &lt;/union&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parseUnion(Node e) throws XmlQueryParser.ParseException
  {
    Union u = new Union();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        Relation r = parse(n);
        u.addOperand(r);
      }
    }
    if (u.m_relations.size() < 2)
      throw new XmlQueryParser.ParseException("Union of less than 2 relations");
    return u;
  }
  
  /**
   * Parse an intersection. XML syntax:
   * <pre>
   * &lt;intersection&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &hellip;
   * &lt;/intersection&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parseIntersection(Node e) throws XmlQueryParser.ParseException
  {
    Intersection u = new Intersection();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        Relation r = parse(n);
        u.addOperand(r);
      }
    }
    if (u.m_relations.size() < 2)
      throw new XmlQueryParser.ParseException("Intersection of less than 2 relations");
    return u;
  }
  
  /**
   * Parse a Cartesian product. XML syntax:
   * <pre>
   * &lt;product&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &hellip;
   * &lt;/product&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parseProduct(Node e) throws XmlQueryParser.ParseException
  {
    Product p = new Product();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        Relation r = parse(n);
        p.addOperand(r);
      }
    }
    if (p.m_relations.size() < 2)
      throw new XmlQueryParser.ParseException("Product of less than 2 relations");
    return p;
  }
  
  /**
   * Parse a join. XML syntax:
   * <pre>
   * &lt;join&gt;
   *   &lt;condition&gt;&hellip;&lt;/condition&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   *   &lt;operand&gt;&hellip;&lt;/operand&gt;
   * &lt;/join&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Relation parseJoin(Node e) throws XmlQueryParser.ParseException
  {
    Join j = new Join();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        Relation r = parse(n);
        if (j.m_left == null)
          j.setLeft(r);
        else if (j.m_right == null)
          j.setRight(r);
        else
          throw new XmlQueryParser.ParseException("Join of more than 2 relations");
      }
      else if (n.getNodeName().compareToIgnoreCase("condition") == 0)
      {
        Condition c = parseCondition(n);
        j.m_condition = c;
      }
    }
    if (j.m_left == null || j.m_right == null)
      throw new XmlQueryParser.ParseException("Join of less than 2 relations");
    if (j.m_condition == null)
      throw new XmlQueryParser.ParseException("Join without a join condition");
    return j;
  }
  
  /**
   * Parse an equality. XML syntax:
   * <pre>
   * &lt;condition&gt;
   *   &lt;x&gt;&hellip;&lt;/x&gt;
   * &lt;/condition&gt;
   * </pre>
   * where <tt>x</tt> is <tt>equals</tt> (no other conditions
   * implemented at the moment).
   * @param e An XML DOM node
   * @return
   */
  protected static Condition parseCondition(Node e) throws XmlQueryParser.ParseException
  {
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("equals") == 0)
      {
        return parseEquality(n);
      }
      if (n.getNodeName().compareToIgnoreCase("and") == 0)
      {
        return parseNAryCondition(new LogicalAnd(), n);
      }
      if (n.getNodeName().compareToIgnoreCase("or") == 0)
      {
        return parseNAryCondition(new LogicalOr(), n);
      }
    }
    // If we get here, we did not recognize any condition we know
    throw new XmlQueryParser.ParseException("No condition recognized");
  }
  
  protected static Condition parseNAryCondition(NAryCondition c, Node e) throws XmlQueryParser.ParseException
  {
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("condition") == 0)
      {
        Condition c_in = parseCondition(n);
        c.addCondition(c_in);
      }
    }
    return c;
  }
  
  /**
   * Parse an equality. XML syntax:
   * <pre>
   * &lt;equals&gt;
   *   &lt;x&gt;&hellip;&lt;/x&gt;
   *   &lt;y&gt;&hellip;&lt;/y&gt;
   * &lt;/equals&gt;
   * </pre>
   * where <tt>x</tt> and <tt>y</tt> are either <tt>attribute</tt>
   * or <tt>value</tt>
   * @param e An XML DOM node
   * @return
   */
  protected static Equality parseEquality(Node e) throws XmlQueryParser.ParseException
  {
    NodeList nl = e.getChildNodes();
    Literal left = null, right = null;
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      Literal a = null;
      if (n.getNodeName().compareToIgnoreCase("attribute") == 0)
      {
        a = parseAttribute(n);
      }
      else if (n.getNodeName().compareToIgnoreCase("value") == 0)
      {
        a = parseValue(n);
      }
      else
        continue;
      if (left == null)
        left = a;
      else
        right = a;
    }
    if (left == null || right == null)
      throw new XmlQueryParser.ParseException("Missing operand in condition 'equals'");
    return new Equality(left, right);
  }

  /**
   * Parse an attribute. XML syntax:
   * <pre>
   * &lt;attribute&gt;<i>name</i>&lt;/attribute&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Attribute parseAttribute(Node e) throws XmlQueryParser.ParseException
  {
    Attribute a = new Attribute();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("name") == 0)
      {
        a.setName(n.getTextContent().trim());
      }
      if (n.getNodeName().compareToIgnoreCase("table") == 0)
      {
        a.setTableName(n.getTextContent().trim());
      }
    }
    if (a.getName().isEmpty())
      throw new XmlQueryParser.ParseException("Empty attribute name");
    return a;
  }
  
  /**
   * Parse a table. XML syntax:
   * <pre>
   * &lt;table&gt;<i>name</i>&lt;/table&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static VariableTable parseVariableTable(Node e) throws XmlQueryParser.ParseException
  {
    VariableTable vt = new VariableTable();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("name") == 0)
      {
        vt.setName(n.getTextContent().trim());
      }
      if (n.getNodeName().compareToIgnoreCase("site") == 0)
      {
        vt.setSite(n.getTextContent().trim());
      }
      if (n.getNodeName().compareToIgnoreCase("operand") == 0)
      {
        Relation r = parse(n);
        vt.setRelation(r);
      }
    }
    if (vt.getName().isEmpty())
      throw new XmlQueryParser.ParseException("Empty vartable name");
    return vt;
  }
  
  /**
   * Parse a table. XML syntax:
   * <pre>
   * &lt;table&gt;<i>name</i>&lt;/table&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Table parseTable(Node e) throws XmlQueryParser.ParseException
  {
    Schema s = null;
    String table_name = "";
    List<Tuple> tuples = new LinkedList<Tuple>();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("schema") == 0)
      {
        s = parseSchema(n);
      }
      if (n.getNodeName().compareToIgnoreCase("tuple") == 0)
      {
        Tuple t = parseTuple(n);
        tuples.add(t);
      }
    }
    Table tab = new Table(table_name);
    tab.m_schema = s;
    tab.putAll(tuples);
    if (s == null)
      throw new XmlQueryParser.ParseException("Missing schema in projection");
    return tab;
  }
  
  /**
   * Parse a value. XML syntax:
   * <pre>
   * &lt;value&gt;<i>name</i>&lt;/value&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Value parseValue(Node e) throws XmlQueryParser.ParseException
  {
    NodeList nl = e.getChildNodes();
    if (nl.getLength() == 0)
      throw new XmlQueryParser.ParseException("Empty value");
    Node n = nl.item(0);
    return new Value(n.getTextContent().trim());
  }
  
  /**
   * Parse a table tuple. XML syntax:
   * <pre>
   * &lt;tuple&gt;
   *   &lt;attr&gt;<i>value</i>&lt;/attr&gt;
   *   &lt;attr&gt;<i>value</i>&lt;/attr&gt;
   *   &hellip;
   * &lt;/tuple&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Tuple parseTuple(Node e) throws XmlQueryParser.ParseException
  {
    Tuple t = new Tuple();
    NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeType() == Node.ELEMENT_NODE)
      {
        String name = n.getNodeName();
        String value = n.getTextContent().trim();
        t.put(new Attribute(name), new Value(value));
      }
    }
    if (t.size() == 0)
      throw new XmlQueryParser.ParseException("Empty tuple");
    return t;
  }

  /**
   * Parse a schema. XML syntax:
   * <pre>
   * &lt;schema&gt;
   *   &lt;attribute&gt;&hellip;&lt;/attribute&gt;
   *   &lt;attribute&gt;&hellip;&lt;/attribute&gt;
   *   &hellip;
   * &lt;/schema&gt;
   * </pre>
   * @param e An XML DOM node
   * @return
   */
  protected static Schema parseSchema(Node e) throws XmlQueryParser.ParseException
  {
    NodeList nl = e.getChildNodes();
    Schema s = new Schema();
    for (int i = 0; i < nl.getLength(); i++)
    {
      Node n = nl.item(i);
      if (n.getNodeName().compareToIgnoreCase("attribute") == 0)
      {
        Attribute a = parseAttribute(n);
        s.add(a);
      }
    }
    if (s.size() == 0)
      throw new ParseException("Empty schema");
    return s;
  }
  
  /**
   * Exception raised when the parser wants to signal an error.
   * @author sylvain
   */
  public static class ParseException extends Exception
  {
    protected String m_message = "ParseException";
    private static final long serialVersionUID = 1L;
    
    public ParseException(String msg)
    {
      m_message = msg;
    }
    
    @Override
    public String toString()
    {
      return m_message;
    }
  }
}
