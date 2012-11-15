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

import java.util.*;

public class CentralizedCommunicator extends Communicator
{
  protected Map<String,Engine> m_sites;
  
  protected List<Relation> m_results;
  
  protected static final int MAX_LOOPS = 100;
  
  public CentralizedCommunicator()
  {
    super();
    m_sites = new HashMap<String,Engine>();
    m_results = new LinkedList<Relation>();
  }
  
  public void run()
  {
    // Do nothing
  }
  
  public void addSite(Engine e)
  {
    m_sites.put(e.m_siteName, e);
  }
  
  public Engine getSite(String name)
  {
    if (!m_sites.containsKey(name))
      return null;
    return m_sites.get(name);
  }
  
  /**
   * Iterates through one loop of communication and processing
   * between sites. The method returns false when no site has
   * processed new data, i.e. when there is nothing left to do.
   * @return True if any site has produced new data, false otherwise
   */
  protected boolean loop()
  {
    Set<Relation> processed = new HashSet<Relation>();
    for (String s_name : m_sites.keySet())
    {
      // Have every site process any pending queries
      Engine e = m_sites.get(s_name);
      Set<Relation> pq = e.processPendingQueries();
      processed.addAll(pq);
    }
    for (Relation r : processed)
    {
      // This site has produced new results: dispatch any
      // of them to other sites as needed
      if (r.isFragment())
      {
        VariableTable vt = (VariableTable) r;
        String destination_name = vt.getSite();
        Engine destination = m_sites.get(destination_name);
        destination.addQuery(vt);
      }
      else
      {
        // The result is not topped with a placeholder:
        m_results.add(r);
      }
    }
    // Return true if any site has created new data in this cycle
    return processed.size() > 0; 
  }
  
  public QueryProcessor getQueryProcessor(Relation query)
  {
    // Pick a site
    return getQueryProcessor(query, "Site 1");
  }
  
  public QueryProcessor getQueryProcessor(Relation query, String site)
  {
    Engine source = m_sites.get(site);
    QueryPlan qp = source.getQueryPlan(query);
    return getQueryProcessor(qp);
  }
  
  public QueryProcessor getQueryProcessor(QueryPlan qp)
  {
    return new CentralizedQueryProcessor(qp);
  }
  
  protected class CentralizedQueryProcessor extends QueryProcessor
  {
    protected QueryPlan m_queryPlan;
    
    protected Relation m_result;
    
    public CentralizedQueryProcessor(QueryPlan qp)
    {
      super();
      m_result = null;
      m_queryPlan = qp;
    }
    
    @Override
    public synchronized void run()
    {
      // Dispatch pieces of the plan to their respective site
      assert m_queryPlan != null;
      for (String site_name : m_queryPlan.keySet())
      {
        Set<Relation> queries = m_queryPlan.get(site_name);
        Engine e = m_sites.get(site_name);
        e.addQuery(queries);
      }
      
      // Loop until result comes back to target site
      // This is why the run method for this processor is declared
      // synchronized: we disallow concurrent accesses to the
      // underlying CentralizedCommunicator
      for (int i = 1; loop() && i < MAX_LOOPS; i++)
      {
        // Do nothing
      }
      assert m_results.size() > 0;
      m_result = m_results.get(0);
    }

    @Override
    public Relation getResult()
    {
      return m_result;
    }
    
  }
}
