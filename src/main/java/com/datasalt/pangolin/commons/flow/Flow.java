package com.datasalt.pangolin.commons.flow;

import java.util.HashMap;
import java.util.Set;

import com.google.common.collect.Maps;

/**
 * A class for creating flows of tasks. The current implementation
 * allows for splits (forks of executions) and barriers (all tasks must
 * reach the barrier before the flow continues).
 * <br/>
 * For creating a Task, you must implement the {@link Executable}. Some
 * configuration can be given to the Executables.
 * <br/>
 * The current implementation does not execute splits nor barriers in
 * pararell. It just execute them sequentially, but assures the correct
 * execution order.
 * <br/>
 * 
 *  An example of use:
 *  <br/>
 *  <code>
 *  [...]
 *  
 * Flow<String> flowRoot = Flow.newFlow();
 * Flow<String> sf1 = new Flow<String>().chain("c1", "c1des", exA).chain("c2", "c2des", exB);
 * Flow<String> sf2 = new Flow<String>().chain("c3", "c3des", exC);
 * flowRoot.split(sf1, sf2);
 * 
 * flowRoot.execute("Config String");
 *  
 *  </code>
 *  <br/>
 *  Another example with barriers:
 *  <br/>
 *  <code>
 * 
 * 	flowRoot = Flow.newFlow();
 *  flowRoot.barrier("Barrier", "b", sf1, sf2).chain("c4", "c4des", exA);
 *  
 *  flowRoot.execute("Config String");
 *  </code>
 *  <br/>
 *  For that case, the task c4 is executed only when sf1 and sf2 are fully executed. 
 * 
 * @author ivan
 *
 * @param <ConfigData>
 */
public class Flow<ConfigData> {

	
	public static final String ROOT_FLOW_NAME = "Flow Root";
	ConfigData config;
	
	ChainableExecutable<ConfigData> root = null;
	ChainableExecutable<ConfigData> current = null;
	
	HashMap<String, ChainableExecutable<ConfigData>> taskByName = Maps.newHashMap();

	public Flow() {
	}
	
	public Flow<ConfigData> chain(String name, String description, Executable<ConfigData> ex){
		current = new ChainableExecutable<ConfigData>(name, description, current, ex);
		
		if (root == null) {
			root = current;
		}
		
		taskByName.put(name, current);
		return this;
	}
	
	public Flow<ConfigData> chain(Flow<ConfigData> flow) {
		if (flow.root == null) {
			return this;
		}
		
		if (root == null) {
			root = flow.root;			
		} else {
			current.registerChild(flow.root);
			flow.current.parent = current;
		}
		
		taskByName.putAll(flow.taskByName);
		current = flow.current;
		
		return this;
	}
	
	public Flow<ConfigData> barrier(String name, String description, Flow<ConfigData> ... flows) {
		BarrierExecutable<ConfigData> barrier = new BarrierExecutable<ConfigData>(name, description, current); 
		current = barrier;
		
		if (root == null) {
			root = current;
		}
		
		taskByName.put(name, current);
		for (Flow<ConfigData> flow: flows) {
			if (flow.root != null) {
				barrier.addBarriedChild(flow.root);
				flow.root.parent = current;
				taskByName.putAll(flow.taskByName);
			}
		}
		
		return this;
	}

	public Flow<ConfigData> split(Flow<ConfigData> ...flows) {
		if (root == null) {
			root = new ChainableExecutable<ConfigData>(ROOT_FLOW_NAME, "Root split", null, null);
			current = root;
			taskByName.put(ROOT_FLOW_NAME, current);
		}
		
		for (Flow<ConfigData> flow : flows) {
			if (flow.root != null) {
				current.registerChild(flow.root);
				flow.root.parent = current;
				taskByName.putAll(flow.taskByName);
			}
		}
		return this;
	}
	
	public void execute(ConfigData config) throws Exception {
		if (root != null) {
			root.execute(config);
		}
	}
	
	public void executeFrom(String name, ConfigData config) throws Exception {
		ChainableExecutable<ConfigData> node = taskByName.get(name);
		if (node != null) {
			node.execute(config);
		}
	}
	
	public Set<String> getNames() {
		return taskByName.keySet();		
	}
	
	public String getDescription(String name) {
		ChainableExecutable<ConfigData> node = taskByName.get(name);
		if (node != null) {
			return node.getDescription();
		}
		return "";
	}
	
	public String getHelp() {
		StringBuilder sb = new StringBuilder();

		for(String name: taskByName.keySet()) {
			sb.append("-------\n");
			ChainableExecutable<ConfigData> c = taskByName.get(name);
			sb.append("Name: " + name + "\n");
			
			ChainableExecutable<ConfigData> parent = c.getParent();
			if (parent != null) {
				sb.append("Parent: " + parent.getName() + "\n");
			}
			
			if (c instanceof BarrierExecutable<?>) {
				for (Executable<ConfigData> bChild: ((BarrierExecutable<ConfigData>) c).barrierFlows ) {
					if (bChild instanceof ChainableExecutable<?>) {
						ChainableExecutable<ConfigData> ex = (ChainableExecutable<ConfigData>) bChild;
						sb.append("BarrierChild: " + ex.getName() + "\n");
					}					
				}
			}
			
			if (c instanceof BarrierExecutable<?>) {
				for (Executable<ConfigData> child: ((BarrierExecutable<ConfigData>) c).childs ) {
					if (child instanceof ChainableExecutable<?>) {
						ChainableExecutable<ConfigData> ex = (ChainableExecutable<ConfigData>) child;
						sb.append("Child: " + ex.getName() + "\n");
					}					
				}
			}
			
			sb.append("Description: " + c.getDescription() + "\n");
			
		}
		
		sb.append("-------\n");
		return sb.toString();
	}
	
	public String toGraphviz() {
		if (root == null) {
			return "";
		}
		GvizNode n = root.genGviz();
		
		StringBuilder sb = new StringBuilder();
		sb.append("digraph Flow {\n");
		for(String edge: n.edges) {
			sb.append(edge + ";\n");
		}
		
		sb.append("}\n");
		return sb.toString();
	}
	
	public static<T> Flow<T> newFlow() {
		return new Flow<T>();
	}
	
	public static void main(String args []) throws Exception {
	}
}
