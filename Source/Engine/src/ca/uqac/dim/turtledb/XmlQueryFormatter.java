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

import java.io.StringWriter;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.*;

/**
 * Facilities to convert relations into equivalent XML representations.
 * The XmlQueryFormatter works in pair with the {@link XmlQueryParser}; more
 * precisely, for any {@link Relation} <tt>r</tt>, we should have that
 * <code>
 * XmlQueryParser.parse(XmlQueryFormatter.toXmlDocument(r)) == r
 * </code> 
 * @author sylvain
 *
 */
public class XmlQueryFormatter
{
  /**
   * Serializes a relation as a DOM document.
   * @param q The relation to serialize
   * @return The resulting XML document
   */
  public static Document toXmlDocument(Relation q)
  {
    XmlQueryVisitor v = new XmlQueryVisitor();
    try
    {
      q.accept(v);
    }
    catch (EmptyQueryVisitor.VisitorException e)
    {
      e.printStackTrace();
    }
    return v.getDocument();
  }
  
  /**
   * Serializes a relation as a string containing an XML representation
   * of the relation.
   * @param q The relation to serialize
   * @return A "stringified" XML document
   */
  public static String toXmlString(Relation q)
  {
    Document doc = toXmlDocument(q);
    try
    {
       DOMSource domSource = new DOMSource(doc);
       StringWriter writer = new StringWriter();
       StreamResult result = new StreamResult(writer);
       TransformerFactory tf = TransformerFactory.newInstance();
       tf.setAttribute("indent-number",2);
       Transformer transformer = tf.newTransformer();
       transformer.setOutputProperty(OutputKeys.INDENT, "yes");
       transformer.transform(domSource, result);
       return writer.toString();
    }
    catch(TransformerException ex)
    {
       ex.printStackTrace();
       return null;
    }
  }
}
