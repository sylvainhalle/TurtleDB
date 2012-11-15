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
 * Visitor that checks if a query is ready to be processed.
 * This is the case when all leaves of the query tree are instances
 * of {@link Table} (and not {@link VariableTable}).
 * @author sylvain
 *
 */
public class ReadyToProcessVisitor extends EmptyQueryVisitor
{ 
  protected boolean m_hasNonTableLeaf = false;
  
  /**
   * Determines if visited query is ready to be processed
   * @return
   */
  public boolean isReady()
  {
    return !m_hasNonTableLeaf;
  }
  
  @Override
  public void visit(VariableTable t)
  {
    if (t.isLeaf())
      m_hasNonTableLeaf = true;
  }
}
