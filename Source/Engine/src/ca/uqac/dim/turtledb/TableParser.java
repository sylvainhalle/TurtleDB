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
 * Parses a table from a character string.
 * @author sylvain
 *
 */
public class TableParser
{
  /**
   * Builds a table from a character string. The string must follow these
   * conventions:
   * <ul>
   * <li>Any leading or trailing whitespace on a line is ignored</li>
   * <li>Empty lines and lines starting with <tt>#</tt> or <tt>---</tt>
   * are ignored</li>
   * <li>The first non-ignored line is a comma- or space-separated list
   * of attribute names</li>
   * <li>The remaining non-ignored lines are comma- or space-separated
   * list of attribute values, making a tuple</li>
   * </ul>
   * @param name The table's name
   * @param data The data
   */
  public static Table parseFromCsv(String name, String s)
  {
    Table out = new Table(name);
    Schema sch = null;
    String[] lines = s.split("\n");
    boolean first_line = true;
    for (String line : lines)
    {
      line = line.trim();
      if (line.startsWith("#") || line.startsWith("---"))
          continue;
      if (first_line)
      {
        first_line = false;
        sch = new Schema(name, line);
        out.setSchema(sch);
        continue;
      }
      Tuple t = new Tuple(sch, line);
      out.put(t);
    }
    out.setName(name);
    return out;
  }
}
