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

import java.util.Iterator;

public abstract class RelationCacheIterator implements RelationIterator
{
  protected Table m_intermediateResult;
  
  private boolean m_called;
  
  private Iterator<Tuple> m_internalIterator;
  
  public RelationCacheIterator()
  {
    super();
    m_called = false;
  }
  
  @Override
  public final boolean hasNext()
  {
    if (!m_called)
      initialize();
    return m_internalIterator.hasNext();
  }
  
  @Override
  public final Tuple next()
  {
    if (!m_called)
      initialize();
    return m_internalIterator.next();    
  }
  
  protected final void initialize()
  {
    getIntermediateResult();
    m_internalIterator = m_intermediateResult.tupleIterator();
    m_called = true;  
  }
  
  @Override
  public void reset()
  {
    m_internalIterator = m_intermediateResult.tupleIterator();
  }
  
  @Override
  public final void remove()
  {
    // Unsupported at the moment
  }
  
  protected abstract void getIntermediateResult();
}