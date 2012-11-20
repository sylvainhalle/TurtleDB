import ca.uqac.dim.turtledb.Table;
import ca.uqac.dim.turtledb.TableParser;
import ca.uqac.dim.turtledb.Union;


public class SimpleDemo
{
  public static void main(String[] args)
  {
    Table r = TableParser.parseFromCsv("A", "a,b,c\n0,0,0\n1,3,4\n0,1,1\n0,2,3\n1,2,3");
  Table r2 = TableParser.parseFromCsv("A", "a,b,c\n0,0,0\n1,2,3\n0,1,1\n0,2,3\n1,3,4");
  Union u2 = new Union();
  u2.addOperand(r);
  u2.addOperand(r2);
  System.out.println(u2);
      /*
  LogicalOr c = new LogicalOr();
  c.addCondition(new Equality(new Attribute("A", "a"), new Value("0")));
  c.addCondition(new Equality(new Attribute("A", "c"), new Value("4")));
  Relation sel = new Selection(c, r);
  Schema sch = new Schema("A.a");
  Relation pro = new Projection(sch, sel);
  Product u = new Product();
  u.addOperand(pro);
  u.addOperand(r2);
       */
      //u.setCondition(new Equality(new Attribute("A", "a"), new Attribute("B", "a")));
      //u.addOperand(new VariableTable("B"));
      //System.out.println(u);
      //System.out.println(XmlQueryFormatter.toXmlString(pro));
      //System.out.println(GraphvizQueryFormatter.toGraphviz(u));
  }
}
