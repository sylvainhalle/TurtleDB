/*-------------------------------------------------------------------------
    Simple distributed database engine
    Copyright (C) 2012-2020  Sylvain Hallé

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

public abstract class QueryVisitor
{
  public abstract void visit(Projection r) throws VisitorException;
  
  public abstract void visit(Selection r) throws VisitorException;
  
  public abstract void visit(Table r) throws VisitorException;
  
  public abstract void visit(VariableTable r) throws VisitorException;
  
  public abstract void visit(Union r) throws VisitorException;
  
  public abstract void visit(Intersection r) throws VisitorException;
  
  public abstract void visit(Join r) throws VisitorException;
  
  public abstract void visit(Product r) throws VisitorException;
  
  public abstract void visit(Renaming r) throws VisitorException;
  
  /**
   * Exception raised when a visitor wants to signal an error.
   * @author sylvain
   */
  public class VisitorException extends Exception
  {
    protected String m_message = "VisitorException";
    private static final long serialVersionUID = 1L;
    
    public VisitorException(String msg)
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
