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

public class TableLinkVisitor extends EmptyQueryVisitor
{
  protected Map<String,Relation> m_tables;
  
  public TableLinkVisitor(VariableTable table)
  {
    m_tables = new HashMap<String,Relation>();
    m_tables.put(table.getName(), table);
  }
  
  public TableLinkVisitor(Map<String,Relation> tables)
  {
    m_tables = tables;
  }
  
  public void visit(VariableTable t) //throws EmptyQueryVisitor.VisitorException
  {
    String table_name = t.getName();
    Relation r = m_tables.get(table_name);
    /*if (r == null)
      throw new EmptyQueryVisitor.VisitorException("Table " + table_name + " cannot be found");*/
    if (r != null)
      t.setRelation(r);
  }
}
