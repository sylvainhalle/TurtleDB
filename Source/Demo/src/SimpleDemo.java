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
import ca.uqac.dim.turtledb.*;

/**
 * Shows an example of basic relation processing with TurtleDB
 * @author sylvain
 *
 */
public class SimpleDemo
{
  public static void main(String[] args)
  {
    // Load tables from CSV strings
    Table r = TableParser.parseFromCsv("A", "a,b,c\n0,0,0\n1,3,4\n0,1,1\n0,2,3\n1,2,3");
    Table r2 = TableParser.parseFromCsv("A", "a,b,c\n0,0,0\n1,2,3\n0,1,1\n0,2,3\n1,3,4");
    
    // Example 1: print union of both tables
    {
      Union u2 = new Union();
      u2.addOperand(r);
      u2.addOperand(r2);
      System.out.println(u2);
    }
    
    // Example 2: a more complex query with disjunctive selection and renaming
    {
      LogicalOr c = new LogicalOr();
      c.addCondition(new Equality(new Attribute("A", "a"), new Value("0")));
      c.addCondition(new Equality(new Attribute("A", "c"), new Value("4")));
      Relation sel = new Selection(c, r);
      Schema sch = new Schema("A.a");
      Relation pro = new Projection(sch, sel);
      Renaming ren = new Renaming(pro);
      ren.rename(new Attribute("A.a"), new Attribute("A.z"));
      Product u = new Product();
      u.addOperand(ren);
      u.addOperand(r2);  
      
      // Create relational algebra tree and output
      // it in various formats
      System.out.println(u);
      System.out.println(XmlQueryFormatter.toXmlString(pro));
      System.out.println(GraphvizQueryFormatter.toGraphviz(u));
    }
  }
}
