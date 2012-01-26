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


public class Tests {

	public static final String NAMESPACE = "com.datasalt";
	
	public static void main(String[] args) throws CoGrouperException, JsonGenerationException, JsonMappingException, IOException{
		CoGrouperConfigBuilder b = CoGrouperConfigBuilder.newOne();
		
		List<Field> userFields = new ArrayList<Field>();
		userFields.add(new Field("user_id", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));
		userFields.add(new Field("name", Schema.create(Type.STRING), null, null, Field.Order.DESCENDING));
		userFields.add(new Field("age", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));
		userFields.add(new Field("my_bytes", Schema.create(Type.BYTES), null, null, Field.Order.DESCENDING));
		
		List<Field> countryFields = new ArrayList<Field>();
		countryFields.add(new Field("user_id", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));
		countryFields.add(new Field("country", Schema.create(Type.STRING), null, null, Field.Order.DESCENDING));
		countryFields.add(new Field("num_people", Schema.create(Type.INT), null, null, Field.Order.DESCENDING));
		countryFields.add(new Field("another", Schema.create(Type.BYTES), null, null, Field.Order.DESCENDING));
		
		Schema usersSchema = Schema.createRecord("usuarios", null, NAMESPACE, false);
		usersSchema.setFields(userFields);
		
		Schema countriesSchema = Schema.createRecord("countries", null, NAMESPACE, false);
		countriesSchema.setFields(countryFields);
		
		b.addSource("usuarios", usersSchema);
		b.addSource("countries",countriesSchema);
		b.setGroupByFields("user_id");
		b.setCommonOrdering(new Ordering().add("user_id",Order.DESCENDING));
		b.setInterSourcesOrdering(Order.DESCENDING);
		b.setIndividualSourceOrdering("usuarios", new Ordering().add("name",Order.ASCENDING).add("age",Order.DESCENDING));
		b.setIndividualSourceOrdering("countries",new Ordering().add("country", Order.DESCENDING));
		
		CoGrouperConfig config = b.build();
		
		ObjectMapper mapper = new ObjectMapper();
		config.toStringAsJSON(mapper);
		
	}
	
	
}
