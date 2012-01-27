import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Ordering;
import com.datasalt.avrool.SerializationInfo;


public class Tests {

	public static final String NAMESPACE = "com.datasalt";
	
	public static void main(String[] args) throws CoGrouperException, JsonGenerationException, JsonMappingException, IOException{
		CoGrouperConfigBuilder b = CoGrouperConfigBuilder.newOne();
		
		List<Field> userFields = new ArrayList<Field>();
		userFields.add(new Field("user_id", Schema.create(Type.INT), null, null, Order.DESCENDING));
		userFields.add(new Field("name", Schema.create(Type.STRING), null, null, Order.DESCENDING));
		userFields.add(new Field("age", Schema.create(Type.INT), null, null, Order.DESCENDING));
		userFields.add(new Field("my_bytes", Schema.create(Type.BYTES), null, null, Order.DESCENDING));
		
		List<Field> countryFields = new ArrayList<Field>();
		countryFields.add(new Field("user_id", Schema.create(Type.INT), null, null, Order.DESCENDING));
		countryFields.add(new Field("name", Schema.create(Type.STRING), null, null, Order.DESCENDING));
		countryFields.add(new Field("country", Schema.create(Type.STRING), null, null, Order.DESCENDING));
		countryFields.add(new Field("num_people", Schema.create(Type.INT), null, null, Order.DESCENDING));
		countryFields.add(new Field("another", Schema.create(Type.BYTES), null, null, Order.IGNORE));
		
		Schema usersSchema = Schema.createRecord("usuarios", null, NAMESPACE, false);
		usersSchema.setFields(userFields);
		
		Schema countriesSchema = Schema.createRecord("countries", null, NAMESPACE, false);
		countriesSchema.setFields(countryFields);
		
		b.addSource(usersSchema);
		b.addSource(countriesSchema);
		b.setGroupByFields("user_id");
		b.setCommonOrdering(new Ordering().add("user_id",Order.DESCENDING).add("name",Order.DESCENDING));
		b.setInterSourcesOrdering(Order.DESCENDING);
		b.setIndividualSourceOrdering("usuarios", new Ordering().add("age",Order.DESCENDING));
		b.setIndividualSourceOrdering("countries",new Ordering().add("country", Order.DESCENDING));
		
		CoGrouperConfig config = b.build();
		
		
		
		
		
		
		ObjectMapper mapper = new ObjectMapper();
		
		System.out.println(config.toString());
		System.out.println(config.toJSON(mapper));
		
		
		
		CoGrouperConfig deserializedConfig = CoGrouperConfig.parseJSON(config.toJSON(mapper), mapper);
		
		
		System.out.println("deserialized");
		System.out.println(deserializedConfig.toString());
		
		SerializationInfo serInfo = SerializationInfo.get(config);
		System.out.println("Common schema " + serInfo.getCommonSchema());
		System.out.println("Particular schemas " + serInfo.getParticularSchemas());
		System.out.println("Intermediate schema " + serInfo.getIntermediateSchema());
		
		
	}
	
	
}
