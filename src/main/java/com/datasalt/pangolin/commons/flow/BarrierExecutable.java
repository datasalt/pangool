package com.datasalt.pangolin.commons.flow;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that executes the Barrier Childs before executing
 * the rest the of the childs. It serves as a barrier for some
 * flows that needs all of them to be finished to continue
 * with the flow. 
 * 
 * @author ivan
 * @param <ConfigData>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class BarrierExecutable<ConfigData> extends ChainableExecutable<ConfigData> {

	ArrayList<Executable<ConfigData>> barrierFlows = new ArrayList<Executable<ConfigData>>();
	
	public BarrierExecutable(String name, String description, ChainableExecutable<ConfigData> father) {
	  super(name, description, father, null);
  }
	
	@Override
  public void execute(ConfigData configData) throws Exception {
		log.info("Barrier [" + name + "]. Executing childs.");
		
		for (Executable<ConfigData> e : barrierFlows) {
			e.execute(configData);
		}
		
		log.info("Barrier [" + name + "] meet.");
		super.execute(configData);
  }
	
	public void addBarriedChild(Executable<ConfigData> barriedChild) {
		barrierFlows.add(barriedChild);
	}
	
	protected GvizNode genGviz() {
		GvizNode ret = new GvizNode();
		String meetingPoint = getName()+"_meeting_point";
		
		for (Executable<ConfigData> e : barrierFlows) {			
			if (e instanceof ChainableExecutable) {				
				ChainableExecutable<?> c = (ChainableExecutable<?>) e;
				
				ret.edges.add(getName() + " -> " +  c.getName());
				
				GvizNode n = c.genGviz();
				ret.edges.addAll(n.edges);
				
				ret.edges.add(n.tail + " -> " + meetingPoint);
			}			
		}
		
		String tail = meetingPoint;
		for (Executable<ConfigData> e : childs) {
			if (e instanceof ChainableExecutable) {				
				ChainableExecutable<?> c = (ChainableExecutable<?>) e;
				
				ret.edges.add(meetingPoint + " -> " + c.getName());
				
				GvizNode n = c.genGviz();
				if (tail == meetingPoint) {
					tail = n.tail;
				}
				ret.edges.addAll(n.edges);
			}			
		}
		ret.head = getName();
		ret.tail = tail;
		return ret;
	}
	
	public static void main(String args[]) throws Exception {
		final AtomicInteger count = new AtomicInteger();
		Executable<Object> e = new Executable() {

			@Override
      public void execute(Object config) throws Exception {
				log.info("Yeah " + count.addAndGet(1));
      }
			
		};
		
    ChainableExecutable r = new ChainableExecutable("u1", null,  null, e);
		BarrierExecutable b = new BarrierExecutable("u.2", null, r);
		ChainableExecutable c = new ChainableExecutable("u1.1", null, b, e);
		c = new ChainableExecutable("u1.1.1", null, c,  e);
		c = new ChainableExecutable("u1.2", null, b, e);
			
		r.execute(null);
	}

}
