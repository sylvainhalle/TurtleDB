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

/**
 * A VariableTable is a placeholder for an actual relation.
 * It is used to denote fragments of a query tree that are to
 * be received from or sent to another site.
 * @author sylvain
 *
 */
public class VariableTable extends UnaryRelation
{
  /**
   * The fragment's name
   */
  protected String m_name;
  
  /**
   * The fragment's site
   */
  protected String m_site;
  
  protected VariableTable()
  {
    super();
    m_name = "";
    m_site = "";
  }
  
  public VariableTable(String name)
  {
    this();
    m_name = name;
  }
  
  public VariableTable(String name, String site)
  {
    this();
    m_name = name;
    m_site = site;
  }
  
  /**
   * Sets the fragment's site.
   * @see {@link getSite}
   * @param site
   */
  public void setSite(String site)
  {
    m_site = site;
  }
  
  /**
   * Gets the fragment's site. If the VariableTable is a leaf
   * in a query tree, it is placeholder for data that will
   * be <em>received</em> from this site. If the VariableTable is
   * the root of the query tree, it indicates that the results
   * of the computation are to be <em>sent</em> to this site.
   */
  public String getSite()
  {
    return m_site;
  }

  @Override
  protected Tuple internalNext()
  {
    if (m_relation != null)
      return m_relation.internalNext();
    else
      return null;
  }

  @Override
  public Schema getSchema()
  {
    if (m_relation != null)
      return m_relation.getSchema();
    else
      return null;
  }
  
  @Override
  public void accept(QueryVisitor v) throws EmptyQueryVisitor.VisitorException
  {
    if (m_relation != null)
      m_relation.accept(v);
    v.visit(this);
  }
  
  @Override
  public String toString()
  {
    if (m_relation == null)
      return "?" + m_name;
    return m_relation.toString();
  }
  
  /**
   * Set the table's name.
   * @param name The table's name
   */
  public void setName(String name)
  {
    m_name = name;
  }
  
  /**
   * Gives the table's name
   * @return The table's name
   */
  public String getName()
  {
    return m_name;
  }
  
  @Override
  public int tupleCount()
  {
    if (m_relation == null)
      return 0;
    return m_relation.tupleCount();
  }
  
  @Override
  public final boolean isFragment()
  {
    return true;
  }
  
  @Override
  public boolean equals(Object o)
  {
    if (o == null)
      return false;
    if (!(o instanceof VariableTable))
      return false;
    return equals((VariableTable) o);
  }
  
  public boolean equals(VariableTable t)
  {
    if (t == null)
      return false;
    return t.m_name.compareTo(m_name) == 0;
  }

  @Override
  public boolean isLeaf()
  {
    // A VariableTable is a leaf only when it has not
    // yet been connected to a relation
    if (m_relation == null)
      return true;
    if (m_relation instanceof VariableTable)
    {
      VariableTable vt = (VariableTable) m_relation;
      return vt.isLeaf();
    }
    return false;
  }
}
