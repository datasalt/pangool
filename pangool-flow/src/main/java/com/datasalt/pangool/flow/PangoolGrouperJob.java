package com.datasalt.pangool.flow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;

@SuppressWarnings("serial")
public abstract class PangoolGrouperJob extends PangoolJob {

	transient TupleReducer reducer = new IdentityTupleReducer();
	transient GroupBy groupBy;
	transient OrderBy orderBy = null;
	
	transient Map<String, RichInput> bindedInputs = new HashMap<String, RichInput>();
	transient RichOutput jobOutput;
	transient Map<String, RichOutput> bindedOutputs = new HashMap<String, RichOutput>();
	
	public PangoolGrouperJob(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, GroupBy groupBy) {
		this(name, inputs, parameters, namedOutputs, groupBy, null, null);
	}

	public PangoolGrouperJob(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, GroupBy groupBy, OrderBy orderBy) {
		this(name, inputs, parameters, namedOutputs, groupBy, orderBy, null);
	}

	public PangoolGrouperJob(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, GroupBy groupBy, OrderBy orderBy, String help) {
	  super(name, inputs, parameters, namedOutputs, help);
	  this.reducer = new IdentityTupleReducer();
	  this.groupBy = groupBy;
	  this.orderBy = orderBy;
  }

	public abstract void configure(Map<String, Object> parsedParameters) throws TupleMRException;
	
	protected void bindInput(String inputName, RichInput inputSpec) {
		bindedInputs.put(inputName, inputSpec);
	}
	
	protected void bindOutput(RichOutput outputSpec) {
		jobOutput = outputSpec;
	}
	
	protected void bindOutput(String name, RichOutput outputSpec) {
		bindedOutputs.put(name, outputSpec);
	}
	
	protected void bindReducer(TupleReducer reducer) {
		this.reducer = reducer;
	}
	
	@Override
  public int run(Path outputPath, Map<String, Path> parsedInputs, Map<String, Object> parsedParameters)
      throws Exception {

		configure(parsedParameters);
		
		TupleMRBuilder mr = new TupleMRBuilder(hadoopConf);
		for(Map.Entry<String, RichInput> inputEntry: bindedInputs.entrySet()) {
			RichInput input = inputEntry.getValue();
			String inputName = inputEntry.getKey();
			if(input instanceof HadoopInput) {
				HadoopInput hadoopInput = (HadoopInput)input;
				mr.addInput(parsedInputs.get(inputName), hadoopInput.format, hadoopInput.processor);
				mr.addIntermediateSchema(hadoopInput.intermediateSchema);
			} else if(input instanceof TupleInput) {
				TupleInput tupleInput = (TupleInput)input;
				mr.addTupleInput(parsedInputs.get(inputName), tupleInput.processor);
				mr.addIntermediateSchema(tupleInput.intermediateSchema);
			} 
		}
		
		mr.setTupleReducer(reducer);

		if(jobOutput instanceof HadoopOutput) {
			HadoopOutput hadoopOutput = (HadoopOutput)jobOutput;
			mr.setOutput(outputPath, hadoopOutput.outputFormat, hadoopOutput.key, hadoopOutput.value);			
		} else if(jobOutput instanceof TupleOutput) {
			TupleOutput tupleOutput = (TupleOutput)jobOutput;
			mr.setTupleOutput(outputPath, tupleOutput.outputSchema);			
		} 

		for(Map.Entry<String, RichOutput> namedOutputEntry: bindedOutputs.entrySet()) {
			RichOutput output = namedOutputEntry.getValue();
			String outputName = namedOutputEntry.getKey();
			if(output instanceof HadoopOutput) {
				HadoopOutput hadoopOutput = (HadoopOutput)output;
				mr.addNamedOutput(outputName, hadoopOutput.outputFormat, hadoopOutput.key, hadoopOutput.value);			
			} else if(output instanceof TupleOutput) {
				TupleOutput tupleOutput = (TupleOutput)output;
				mr.addNamedTupleOutput(outputName, tupleOutput.outputSchema);			
			}			
		}
		
		mr.setGroupByFields(groupBy.groupByFields);
		if(orderBy != null) {
			mr.setOrderBy(orderBy);
		}

		return executeCoGrouper(mr);
  }
}
