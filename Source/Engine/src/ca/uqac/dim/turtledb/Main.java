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

/**
 * @author sylvain
 *
 */
/*package*/ class Main
{
  /**
   * Only show a version number, in case someone <em>runs</em>
   * the jar instead of linking to it
   * @param args
   */
  public static void main(String[] args)
  {
    System.out.println("TurtleDB version 1.0.3");
    System.out.println("(C) 2012-2020 Sylvain Hallé, Université du Québec à Chicoutimi\n");
    System.exit(0);
  }
  
  private Main()
  {
    throw new UnsupportedOperationException("Cannot instantiate singleton class Main");
  }
}
