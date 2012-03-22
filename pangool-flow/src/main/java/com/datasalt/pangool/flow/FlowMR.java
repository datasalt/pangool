package com.datasalt.pangool.flow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.flow.io.HadoopInput;
import com.datasalt.pangool.flow.io.HadoopOutput;
import com.datasalt.pangool.flow.io.RichInput;
import com.datasalt.pangool.flow.io.RichOutput;
import com.datasalt.pangool.flow.io.TupleInput;
import com.datasalt.pangool.flow.io.TupleOutput;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;

@SuppressWarnings("serial")
public abstract class FlowMR extends FlowJob {

	@SuppressWarnings("rawtypes")
  transient TupleReducer reducer = new IdentityTupleReducer();
	transient GroupBy groupBy;
	transient OrderBy orderBy = null;
	
	transient Map<String, RichInput> bindedInputs = new HashMap<String, RichInput>();
	transient RichOutput jobOutput;
	transient Map<String, RichOutput> bindedOutputs = new HashMap<String, RichOutput>();
	
	public FlowMR(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, GroupBy groupBy) {
		this(name, inputs, parameters, namedOutputs, groupBy, null, null);
	}

	public FlowMR(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, GroupBy groupBy, OrderBy orderBy) {
		this(name, inputs, parameters, namedOutputs, groupBy, orderBy, null);
	}

	public FlowMR(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, GroupBy groupBy, OrderBy orderBy, String help) {
	  super(name, inputs, parameters, namedOutputs, help);
	  this.reducer = new IdentityTupleReducer();
	  this.groupBy = groupBy;
	  this.orderBy = orderBy;
  }

	public abstract void configure(Map<String, Object> parsedParameters) throws TupleMRException;
	
	protected void addInput(String inputName, RichInput inputSpec) {
		bindedInputs.put(inputName, inputSpec);
	}
	
	protected void setOutput(RichOutput outputSpec) {
		jobOutput = outputSpec;
	}
	
	protected void setOutput(String name, RichOutput outputSpec) {
		bindedOutputs.put(name, outputSpec);
	}
	
	protected void setReducer(@SuppressWarnings("rawtypes") TupleReducer reducer) {
		this.reducer = reducer;
	}
	
	protected TupleMRBuilder getMRBuilder() {
		return mr;
	}
	
	transient TupleMRBuilder mr;
	
	@SuppressWarnings("unchecked")
  @Override
  public int run(Path outputPath, Map<String, Path> parsedInputs, Map<String, Object> parsedParameters)
      throws Exception {

		mr = new TupleMRBuilder(hadoopConf);
		
		configure(parsedParameters);

		for(Map.Entry<String, RichInput> inputEntry: bindedInputs.entrySet()) {
			RichInput input = inputEntry.getValue();
			String inputName = inputEntry.getKey();
			if(input instanceof HadoopInput) {
				HadoopInput hadoopInput = (HadoopInput)input;
				mr.addInput(parsedInputs.get(inputName), hadoopInput.getFormat(), hadoopInput.getProcessor());
				for(Schema schema: hadoopInput.getIntermediateSchemas()) {
					mr.addIntermediateSchema(schema);
				}
			} else if(input instanceof TupleInput) {
				TupleInput tupleInput = (TupleInput)input;
				mr.addTupleInput(parsedInputs.get(inputName), tupleInput.getProcessor());
				for(Schema schema: tupleInput.getIntermediateSchemas()) {
					mr.addIntermediateSchema(schema);
				}
			} 
		}
		
		mr.setTupleReducer(reducer);

		if(jobOutput instanceof HadoopOutput) {
			HadoopOutput hadoopOutput = (HadoopOutput)jobOutput;
			mr.setOutput(outputPath, hadoopOutput.getOutputFormat(), hadoopOutput.getKey(), hadoopOutput.getValue());			
		} else if(jobOutput instanceof TupleOutput) {
			TupleOutput tupleOutput = (TupleOutput)jobOutput;
			mr.setTupleOutput(outputPath, tupleOutput.getOutputSchema());			
		} 

		for(Map.Entry<String, RichOutput> namedOutputEntry: bindedOutputs.entrySet()) {
			RichOutput output = namedOutputEntry.getValue();
			String outputName = namedOutputEntry.getKey();
			if(output instanceof HadoopOutput) {
				HadoopOutput hadoopOutput = (HadoopOutput)output;
				mr.addNamedOutput(outputName, hadoopOutput.getOutputFormat(), hadoopOutput.getKey(), hadoopOutput.getValue());			
			} else if(output instanceof TupleOutput) {
				TupleOutput tupleOutput = (TupleOutput)output;
				mr.addNamedTupleOutput(outputName, tupleOutput.getOutputSchema());			
			}			
		}
		
		mr.setGroupByFields(groupBy.groupByFields);
		if(orderBy != null) {
			mr.setOrderBy(orderBy);
		}

		return executeCoGrouper(mr);
  }
}
