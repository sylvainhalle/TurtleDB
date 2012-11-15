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

import ca.uqac.dim.turtledb.*;

public class EngineTest
{
  public static void main(String[] args)
  {
    // Populate a communicator with sites and tables and start it
    Communicator cm = createCommunicator();
    cm.run();
    
    // Create a plan
    QueryPlan qp = new QueryPlan();
    // Plan for site 1
    {
      VariableTable vt = new VariableTable("&alpha;", "Site 2");
      vt.setRelation(new VariableTable("A"));
      qp.put("Site 1", vt);
      // Optional: echo the plan
      //System.out.println(GraphvizQueryFormatter.toGraphviz(vt));
    }
    // Plan for site 2
    {
      Union u = new Union();
      u.addOperand(new VariableTable("&alpha;", "Site 1"));
      u.addOperand(new VariableTable("B", "Site 2"));
      qp.put("Site 2", u);
      // Optional: echo the plan
      //System.out.println(GraphvizQueryFormatter.toGraphviz(u));
    }
    
    // Have the communicator execute the plan
    QueryProcessor p = cm.getQueryProcessor(qp);
    p.run();
    Relation result = p.getResult();
    System.out.println(result);
  }
  
  private static Communicator createCommunicator()
  {
    // Populate site 1
    Engine site_1 = new Engine("Site 1");
    {
      Table r = TableParser.parseFromCsv("A", "a,b,c\n0,0,0\n1,3,4\n0,1,1\n0,2,3\n1,2,3");
      site_1.putRelation("A", r);
    }
    // Populate site 2
    Engine site_2 = new Engine("Site 2");
    {
      Table r = TableParser.parseFromCsv("B", "a,b,c\n0,0,0\n1,3,4\n0,1,1\n0,2,3\n1,2,3");
      site_2.putRelation("B", r);
    }
    
    // Instantiates the centralized communication manager
    CentralizedCommunicator cm = new CentralizedCommunicator();
    cm.addSite(site_1);
    cm.addSite(site_2);
    return cm;
  }
}
