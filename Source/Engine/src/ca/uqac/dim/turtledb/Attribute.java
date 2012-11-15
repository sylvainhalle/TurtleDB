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

public class Attribute extends Literal
{
  protected String m_value;
  protected String m_tableName;
  
  public Attribute()
  {
    m_value = "";
    m_tableName = "";
  }
  
  /**
   * Builds an attribute from a string. If the string contains a dot,
   * the left-hand side is a table name and the right-hand side is
   * the attribute's name. 
   * @param s
   */
  public Attribute(String s)
  {
    this();
    if (!s.contains("."))
      m_value = s;
    else
    {
      String[] parts = s.split("\\.");
      m_tableName = parts[0];
      m_value = parts[1];
    }
  }
  
  public Attribute(String t, String s)
  {
    this();
    m_tableName = t;
    m_value = s;
  }
  
  /**
   * Constructor by copy
   * @param a
   */
  public Attribute(Attribute a)
  {
    this();
    if (a == null)
      return;
    m_value = new String(a.m_value);
    m_tableName = new String(a.m_tableName);
  }
  
  public void setName(String name)
  {
    m_value = name;
  }
  
  public void setTableName(String name)
  {
    m_tableName = name;
  }
  
  public String getName()
  {
    return m_value;
  }
  
  public String getTableName()
  {
    return m_tableName;
  }
  
  @Override
  public String toString()
  {
    String out = m_value;
    if (!m_tableName.isEmpty())
      out = m_tableName + "." + out;
    return out;
  }
  
  @Override
  public boolean equals(Object o)
  {
    if (o == null)
      return false;
    if (!(o instanceof Attribute))
      return false;
    return equals((Attribute) o);
  }
  
  public boolean equals(Attribute a)
  {
    if (a == null)
      return false;
    return m_tableName.compareTo(a.m_tableName) == 0 
        && m_value.compareTo(a.m_value) == 0;
  }
  
  @Override
  public int hashCode()
  {
    return m_tableName.hashCode() + m_value.hashCode();
  }

  @Override
  public int compareTo(Literal o)
  {
    if (o instanceof Value)
      return -1; // All attributes go before all values
    assert o instanceof Attribute;
    return compareTo((Attribute) o);
  }
  
  public int compareTo(Attribute o)
  {
    int compare_table = m_tableName.compareTo(o.m_tableName);
    if (compare_table < 0)
      return -1;
    if (compare_table > 0)
      return 1;
    if (compare_table == 0)
    {
      int compare_attName = m_value.compareTo(o.m_value);
      if (compare_attName == 0)
        return 0;
      if (compare_attName < 0)
        return -1;
      if (compare_attName > 0)
        return 1;
    }
    return 0;
  }
}
