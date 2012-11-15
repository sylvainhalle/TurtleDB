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

public class Value extends Literal
{
  private String m_value;
  
  public Value(String s)
  {
    m_value = s;
  }
  
  @Override
  public String toString()
  {
    return m_value;
  }
  
  @Override
  public boolean equals(Object o)
  {
    if (o == null)
      return false;
    if (!(o instanceof Value))
      return false;
    return equals((Value) o);
  }
  
  public boolean equals(Value v)
  {
    if (v == null)
      return false;
    return m_value.compareTo(v.m_value) == 0;
  }
  
  @Override
  public int hashCode()
  {
    return m_value.hashCode();
  }
  
  @Override
  public int compareTo(Literal o)
  {
    if (o instanceof Attribute)
      return 1; // All attributes go before all values
    assert o instanceof Value;
    return compareTo((Value) o);
  }
  
  public int compareTo(Value o)
  {
    int compare_value = m_value.compareTo(o.m_value);
    if (compare_value < 0)
      return -1;
    if (compare_value > 0)
      return 1;
    return 0;
  }
}
