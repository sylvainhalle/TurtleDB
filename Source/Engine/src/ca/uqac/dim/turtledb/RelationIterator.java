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
import java.util.LinkedList;
import java.util.List;

public abstract class RelationIterator implements Iterator<Tuple>
{
  protected List<Tuple> m_outputTuples;
  protected Tuple m_nextTuple;
  protected boolean m_internalNextCalled;

  public RelationIterator()
  {
    super();
    m_outputTuples = new LinkedList<Tuple>();
    m_internalNextCalled = false;
  }

  @Override
  public final boolean hasNext()
  {
    if (!m_internalNextCalled)
    {
      next();
      m_internalNextCalled = true;
    }
    return m_nextTuple != null;
  }

  @Override
  public final Tuple next()
  {
    if (!m_internalNextCalled)
    {
      while (true)
      {
        m_nextTuple = internalNext();
        if (m_nextTuple == null)
        {
          break;
        }
        if (!m_outputTuples.contains(m_nextTuple))
        {
          m_outputTuples.add(m_nextTuple);
          break;
        }
      }
    }
    m_internalNextCalled = false;
    return m_nextTuple;
  }

  /**
   * Method that must be implemented by every non-abstract
   * relation; it returns the next tuple
   * of the enumeration, if any. Methods {@link next} and
   * {@link hasNext} use the return value of {@link internalNext}
   * and additionally remove any duplicate tuples from the output
   * enumeration. Hence a call to {@link next} may result in
   * multiple calls to the relation's {@link internalNext}, if
   * the tuples returned are already part of the result (this is
   * especially true of {@link Projection}s.
   * @return The next tuple, <tt>null</tt> if no such tuple
   * exists
   */
  protected abstract Tuple internalNext();

  @Override
  public final void remove()
  {
    // Not supported at the moment
  }

  /**
   * Resets the enumeration of tuples, i.e. starts back at
   * the first tuple of the relation.
   */
  public void reset()
  {
    m_nextTuple = null;
    m_outputTuples.clear();
    m_internalNextCalled = false;
  }
}