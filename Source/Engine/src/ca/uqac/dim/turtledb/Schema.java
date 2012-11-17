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

import java.util.Vector;

/**
 * A schema is an ordered list of attributes.
 * @author sylvain
 *
 */
public class Schema extends Vector<Attribute>
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  /*package*/ Schema()
  {
    super();
  }
  
  /**
   * Constructor from a comma-separated string.
   * @param s The schema
   */
  public Schema(String s)
  {
    this();
    String parts[] = s.split(",");
    for (String a : parts)
    {
      a = a.trim();
      Attribute att = new Attribute(a);
      this.add(att);
    }
  }
  
  /**
   * Constructor by copy
   * @param s The schema
   */
  public Schema(Schema s)
  {
    this();
    for (Attribute att : s)
    {
      this.add(att);
    }
  }
  
  /**
   * Constructor from a comma-separated string.
   * @param tableName The table's name
   * @param s The schema
   */
  public Schema(String tableName, String s)
  {
    String parts[] = s.split(",");
    for (String a : parts)
    {
      a = a.trim();
      Attribute att = new Attribute(a);
      att.setTableName(tableName);
      this.add(att);
    }
  }
  
  /**
   * Creates a schema from a comma-separated list of
   * attributes. Each attribute must be formatted
   * as [tablename.]attributename
   * @param s The string containing the list
   */
  protected void createFromString(String s)
  {
    String parts[] = s.split(",");
    for (String a : parts)
    {
      a = a.trim();
      Attribute att = new Attribute(a);
      this.add(att);
    }    
  }
  
  /**
   * Assigns a table name to the tuples in the schema
   * @param name The table's name
   */
  public void setTableName(String name)
  {
	  for (Attribute a : this)
		  a.setTableName(name);
  }

}
