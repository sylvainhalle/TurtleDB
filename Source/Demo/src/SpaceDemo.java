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
import java.io.*;

import ca.uqac.dim.turtledb.*;
import ca.uqac.dim.turtledb.util.FileReadWrite;

/**
 * Shows an example of basic relation processing with TurtleDB
 * @author sylvain
 *
 */
public class SpaceDemo
{
	public static void main(String[] args)
	{
	  // ---------------
      // Step 1: load tables from files
      // ---------------
	  
	  Relation r_Astronaut = null, r_Mission = null, r_Crew = null, r_Rocket = null;
	  try
	  {
	    r_Astronaut = XmlQueryParser.parse(FileReadWrite.getFileContents("data/Space/Astronaut.xml"));
	    r_Mission = XmlQueryParser.parse(FileReadWrite.getFileContents("data/Space/Mission.xml"));
	    r_Crew = XmlQueryParser.parse(FileReadWrite.getFileContents("data/Space/Crew.xml"));
	    r_Rocket = XmlQueryParser.parse(FileReadWrite.getFileContents("data/Space/Rocket.xml"));
	  }
	  catch (FileNotFoundException e)
	  {
	    System.err.println("File not found");
	    System.exit(1);
	  }
	  catch (IOException e)
	  {
	    System.err.println("Error reading files");
	    System.exit(1);
	  }
	  catch (XmlQueryParser.ParseException e)
	  {
        System.err.println("Error parsing XML files");
        System.exit(1);
	  }
	  if (r_Astronaut == null || r_Mission == null || r_Crew == null || r_Rocket == null)
	  {
	    System.err.println("Error reading Space database");
	    System.exit(1);
	  }
	  
	  // ---------------
	  // Step 2: build query trees
	  // ---------------

	  // Example 1: display the name and mission of all Command Pilots
	  {
	    Condition cond_pilot = new Equality(new Attribute("Crew", "Role"), new Value("Command Pilot"));
	    Selection sel_pilot = new Selection(cond_pilot, r_Crew);
	    Join j1 = new Join(new Equality(new Attribute("Crew", "Astronaut"), new Attribute("Astronaut", "Name")));
	    j1.addOperand(r_Astronaut);
	    j1.addOperand(sel_pilot);
	    Schema sch_nameMission = new Schema();
	    sch_nameMission.add(new Attribute("Astronaut", "Name"));
	    sch_nameMission.add(new Attribute("Crew", "Mission"));
	    Projection proj_nameMission = new Projection(sch_nameMission, j1);

	    // We run the computation of the result by printing it
	    System.out.println("Name of all missions' command pilot\n");
	    System.out.println(proj_nameMission);
	  }
	  
	  // Example 2: show name and height of all rockets flown by Virgil Grissom
	  {
	    Condition cond_virgil = new Equality(new Attribute("Crew", "Astronaut"), new Value("Virgil Grissom"));
	    Selection sel_virgil = new Selection(cond_virgil, r_Crew);
	    Table sel_virgil_c = new Table(sel_virgil);
	    Join j1 = new Join(new Equality(new Attribute("Crew", "Mission"), new Attribute("Mission", "Name")));
        j1.addOperand(r_Mission);
        j1.addOperand(sel_virgil_c);
        Join j2 = new Join(new Equality(new Attribute("Mission", "Rocket"), new Attribute("Rocket", "Name")));
        j2.addOperand(j1);
        j2.addOperand(r_Rocket);
        Schema sch_rocket  = new Schema();
        sch_rocket.add(new Attribute("Rocket", "Name"));
        sch_rocket.add(new Attribute("Rocket", "Height"));
        Projection proj_rocket = new Projection(sch_rocket, j2);
        
        // We run the computation of the result by printing it
        System.out.println("Rockets flown by Virgil Grissom\n");
        System.out.println(proj_rocket);
	  }
	}
}
