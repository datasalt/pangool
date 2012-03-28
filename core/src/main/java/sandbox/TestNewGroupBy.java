package sandbox;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Aliases;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.Criteria.Order;

public class TestNewGroupBy {

	public static void main(String[] args) throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		Schema schema1 = new Schema("schema1", Fields.parse("user_id:int,operation:string,age:long,timestamp:int,country:string"));
		Schema schema2 = new Schema("schema2", Fields.parse("id:int,op:string,another_id:int,time:int"));
		
		b.addIntermediateSchema(schema1);
		b.addIntermediateSchema(schema2);
		b.setFieldAliases("schema1",new Aliases().addAlias("user_id","id").addAlias("operation","op"));
		b.setFieldAliases("schema2", new Aliases().addAlias("timestamp","time"));
		b.setGroupByFields("id", "op");
		b.setOrderBy(new OrderBy().add("op", Order.ASC).add("id", Order.DESC)
		    .addSchemaOrder(Order.DESC).add("time", Order.DESC));
		b.setSpecificOrderBy("schema1", new OrderBy().add("blabla", Order.DESC));
		
		TupleMRConfig config = b.buildConf();
		                   
		
		
		b.setGroupByFields("a");
		TupleMRConfig conf = b.buildConf();
		conf.getSerializationInfo();
	}
	
}
