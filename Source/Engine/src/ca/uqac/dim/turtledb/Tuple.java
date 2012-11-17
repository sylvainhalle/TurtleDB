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

/**
 * A tuple is an <em>ordered</em> collection of attribute-value pairs.
 * @author sylvain
 *
 */
public class Tuple implements Comparable<Tuple>
{
  protected Vector<Attribute> m_attributes;
  protected Vector<Value> m_values;
  
  /*package*/ Tuple()
  {
    super();
    m_attributes = new Vector<Attribute>();
    m_values = new Vector<Value>();
  }
  
  public Tuple(Schema sch, Value[] val)
  {
    this();
    assert sch.size() == val.length;
    for (int i = 0; i < val.length; i++)
    {
      this.put(sch.elementAt(i), val[i]);
    }
  }
  
  public Tuple(Schema sch, String values)
  {
    this();
    String parts[] = values.split("[,\\s]");
    assert sch.size() == parts.length;
    for (int i = 0; i < parts.length; i++)
    {
      this.put(sch.elementAt(i), new Value(parts[i]));
    }
  }
  
  public Value get(Literal a)
  {
    // TODO: eventually replace by a Map search
    for (int i = 0; i < m_attributes.size(); i++)
    {
      Attribute att = m_attributes.elementAt(i);
      if (att.equals(a))
        return m_values.elementAt(i);
    }
    return null;
  }
  
  public void clear()
  {
    m_attributes.clear();
    m_values.clear();
  }
  
  public void put(Attribute a, Value v)
  {
    m_attributes.add(a);
    m_values.add(v);
  }
  
  public void putAll(Tuple t)
  {
    m_attributes.addAll(t.m_attributes);
    m_values.addAll(t.m_values);
  }
  
  public Set<Attribute> keySet()
  {
    Set<Attribute> out = new HashSet<Attribute>();
    out.addAll(m_attributes);
    return out;
  }
  
  /**
   * Affixes a given table name to all attributes of the tuple.
   * @param name The table's name
   */
  public void setTable(String name)
  {
    for (Attribute a : m_attributes)
    {
      a.setTableName(name);
    }
  }

  /**
   * Compares two tuples. We use lexicographical ordering of
   * the tuple's values, starting from the left.
   */
  @Override
  public int compareTo(Tuple t)
  {
    if (m_values.size() < t.m_values.size())
      return -1;
    if (m_values.size() > t.m_values.size())
      return 1;
    assert m_values.size() == t.m_values.size();
    for (int i = 0; i < m_values.size(); i++)
    {
      Value v1 = m_values.get(i);
      Value v2 = t.m_values.get(i);
      int comp = v1.compareTo(v2);
      if (comp < 0)
        return -1;
      if (comp > 0)
        return 1;
    }
    return 0;
  }
  
  public int size()
  {
    return m_values.size();
  }
  
  @Override
  public String toString()
  {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < m_values.size(); i++)
    {
      if (i > 0)
        out.append(",");
      Attribute a = m_attributes.get(i);
      Value v = m_values.get(i);
      out.append(a).append("=").append(v);
    }
    return out.toString();
  }
  
  /**
   * Returns the tuple's degree (i.e. number of columns)
   * @return The degree
   */
  public int getDegree()
  {
    return m_attributes.size();
  }
  
  @Override
  public boolean equals(Object o)
  {
    if (o == null)
      return false;
    if (!(o instanceof Tuple))
      return false;
    assert o instanceof Tuple;
    return equals((Tuple) o);
  }
  
  public boolean equals(Tuple t)
  {
    if (t == null)
      return false;
    if (t.m_attributes.size() != m_attributes.size())
      return false;
    if (t.m_values.size() != m_values.size())
      return false;
    for (int i = 0; i < m_values.size(); i++)
    {
      if (!m_attributes.get(i).equals(t.m_attributes.get(i)))
        return false;
      if (!m_values.get(i).equals(t.m_values.get(i)))
        return false;
    }
    return true;
  }
  
  /**
   * Fusions multiple tuples to create a single tuple
   * @param v
   * @return
   */
  public static Tuple makeTuple(Vector<Tuple> v)
  {
    Tuple t = new Tuple();    
    for (Tuple tt : v)
    {
      t.putAll(tt);
    }
    return t;
  }
}