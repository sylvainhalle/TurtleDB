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

import java.io.*;
import java.net.*;
import java.util.*;

public class HttpCommunicator extends Communicator
{

  protected Map<String,SiteInfo> m_siteInfo;

  protected Engine m_engine;

  public HttpCommunicator()
  {
    super();
    m_siteInfo = new HashMap<String,SiteInfo>();
  }

  public void addSiteInfo(String name, String url)
  {
    SiteInfo si = new SiteInfo(name, url);
    m_siteInfo.put(name, si);
  }

  protected void sendQuery(String site_name, Relation r) throws Communicator.QueryExecutionException
  {
    SiteInfo si = m_siteInfo.get(site_name);
    if (si == null)
    {
      throw new Communicator.QueryExecutionException("Unknown site: " + site_name);
    }
    String soap_string = XmlQueryFormatter.toXmlString(r);
    @SuppressWarnings("unused")
    String return_value = "";
    try
    {
      return_value = postData(si.m_siteUrl, soap_string);
    }
    catch (IOException e)
    {
      throw new Communicator.QueryExecutionException("IOException while sending data to site " + site_name);
    }
  }

  protected void sendQuery(String site_name, Set<Relation> rels) throws Communicator.QueryExecutionException
  {
    for (Relation r : rels)
      sendQuery(site_name, r);
  }

  /**
   * Sends a string of data through an TCP connection at a given URL:port
   * @param destination_url The destination URL
   * @param data The data to send
   * @return The response returned by the destination (if any)
   * @throws IOException
   */
  protected String sendData(URL url, String data) throws IOException
  {
    URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
    wr.write(data);
    wr.flush();

    // Get the response
    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line;
    StringBuilder out = new StringBuilder();
    while ((line = rd.readLine()) != null)
    {
      out.append(line).append("\n");
    }
    wr.close();
    rd.close();   
    return out.toString();
  }

  /**
   * Sends a string of data through an TCP connection at a given URL:port
   * using an HTTP POST request
   * @param host The destination host
   * @param location The location on the host (i.e. the page name)
   * @param payload The data to send
   * @return The response returned by the destination (if any)
   * @throws IOException
   */
  protected String postData(String destination_url, String payload) throws IOException
  {
    URL url = new URL(destination_url);
    StringBuilder out = new StringBuilder();
    out.append("POST ").append(url.getFile()).append(" HTTP/1.1\n");
    out.append("Host: ").append(url.getHost()).append("\n");
    out.append("User-Agent: TurtleDB\n");
    out.append("Content-Type: application/xml\n");
    out.append("Content-Length: ").append(payload.length()).append("\n");
    out.append(payload);
    String http_response = sendData(url, out.toString());
    if (!http_response.startsWith("200 OK"))
      throw new IOException("HTTP error code");
    return http_response;
  }

  /**
   * Information about a site
   * @author sylvain
   */
  protected class SiteInfo
  {
    public String m_siteName;
    public String m_siteUrl;

    public SiteInfo(String name, String url)
    {
      super();
      m_siteName = name;
      m_siteUrl = url;
    }
  }

  @Override
  public QueryProcessor getQueryProcessor(Relation query)
  {
    QueryPlan qp = m_engine.getQueryPlan(query);
    return getQueryProcessor(qp);
  }

  @Override
  public QueryProcessor getQueryProcessor(QueryPlan qp)
  {
    return new HttpQueryProcessor(qp);
  }

  protected class HttpQueryProcessor extends QueryProcessor
  {
    protected QueryPlan m_queryPlan;

    protected Relation m_result;

    public HttpQueryProcessor(QueryPlan qp)
    {
      m_queryPlan = qp;
    }

    @Override
    public void run()
    {
      // Dispatch query plan pieces to every site
      for (String site_name : m_queryPlan.keySet())
      {
        Set<Relation> rels = m_queryPlan.get(site_name);
        try
        {
          sendQuery(site_name, rels);
        }
        catch (Communicator.QueryExecutionException e)
        {
          e.printStackTrace();
        }
      }
    }

    @Override
    public Relation getResult()
    {
      return m_result;
    }
  }

  /**
   * Listen to the connection for any incoming messages
   */
  @Override
  public void run()
  {
    int port = 1234;
    ServerSocket serversocket = null;
    try
    {
      serversocket = new ServerSocket(port);
    }
    catch (IOException e)
    {
      // Nothing to do if we can't open a socket
      return;
    }

    //go in a infinite loop, wait for connections, process request, send response
    while (true)
    {
      try
      {
        //this call waits/blocks until someone connects to the port we
        //are listening to
        Socket connectionsocket = serversocket.accept();
        //Read the http request from the client from the socket interface
        //into a buffer.
        BufferedReader input =
            new BufferedReader(new InputStreamReader(connectionsocket.
                getInputStream()));
        //Prepare a outputstream from us to the client,
        //this will be used sending back our response
        //(header + requested file) to the client.
        DataOutputStream output =
            new DataOutputStream(connectionsocket.getOutputStream());
        http_handler(input, output);
      }
      catch (Exception e)
      { 
        e.printStackTrace();
      }
      if (serversocket != null)
      {
        try 
        {
          serversocket.close();
        } 
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }
  }

  //this method makes the HTTP header for the response
  //the headers job is to tell the browser the result of the request
  //among if it was successful or not.
  private String construct_http_header(int return_code)
  {
    String s = "HTTP/1.1 ";
    switch (return_code)
    {
    case 200:
      s = s + "200 OK";
      break;
    case 400:
      s = s + "400 Bad Request";
      break;
    case 403:
      s = s + "403 Forbidden";
      break;
    case 404:
      s = s + "404 Not Found";
      break;
    case 500:
      s = s + "500 Internal Server Error";
      break;
    case 501:
      s = s + "501 Not Implemented";
      break;
    }
    s = s + "\r\n";
    s = s + "Connection: close\r\n"; //we can't handle persistent connections
    s = s + "Server: TurtleDB\r\n"; //server name
    s = s + "\r\n"; //this marks the end of the httpheader
    return s;
  }

  private void http_handler(BufferedReader input, DataOutputStream output)
  {
    int method = 0; //1 post, 0 not supported
    try
    {
      String tmp = input.readLine(); //read from the stream
      if (tmp.startsWith("POST"))
      { //compare it is it GET
        method = 1;
      } //if we set it to method 1
      if (method == 0)
      { // not supported
        output.writeBytes(construct_http_header(501));
        output.close();
        return;
      }
      String line = input.readLine();
      while (line != null)
      {

        line = input.readLine();
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

}
