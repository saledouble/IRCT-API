/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.executable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import edu.harvard.hms.dbmi.bd2k.irct.action.Action;
import edu.harvard.hms.dbmi.bd2k.irct.event.IRCTEventListener;
import edu.harvard.hms.dbmi.bd2k.irct.exception.ResourceInterfaceException;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Result;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.ResultStatus;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.SecureSession;
import edu.harvard.hms.dbmi.bd2k.irct.util.Utilities;

/**
 * A child node in an execution tree that can be executed. It can have children
 * of its own.
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public class ExecutableChildNode implements Executable {

	private SecureSession session;
	private boolean blocking;
	private Action action;
	private Map<String, Executable> children;
	private Executable parent;
	private Map<String, Result> childrenResults;
	private ExecutableStatus state;

	private IRCTEventListener irctEventListener;

	public ExecutableChildNode() {
		this.setChildren(new HashMap<String, Executable>());
		this.childrenResults = new HashMap<String, Result>();
	}

	@Override
	public void setup(SecureSession secureSession) {
		this.session = secureSession;
		this.state = ExecutableStatus.CREATED;

		this.irctEventListener = Utilities.getIRCTEventListener();
	}

	@Override
	public void run() throws ResourceInterfaceException {
		irctEventListener.beforeAction(session, action);

		if (isBlocking() && !getChildren().isEmpty()) {
			runSequentially();
		} else if (!getChildren().isEmpty()) {
			runConcurrently();
		}
		
		
		
		if (!childrenResults.isEmpty()) {
			action.updateActionParams(childrenResults);
		}

		this.state = ExecutableStatus.RUNNING;
		this.action.run(this.session);
		this.state = ExecutableStatus.COMPLETED;

		irctEventListener.afterAction(session, action);
	}

	private void runSequentially() throws ResourceInterfaceException {
		for (String key : this.getChildren().keySet()) {
			Executable executable = this.getChildren().get(key);
			executable.setup(this.session);
			executable.run();
			childrenResults.put(key, executable.getResults());
		}
	}

	private void runConcurrently() {
		try {
			ExecutorService mes = Executors.newFixedThreadPool(20);
			List<Future<ExecutorIdentifier>> childResults = new ArrayList<Future<ExecutorIdentifier>>();
			for (String childId : this.getChildren().keySet()) {
				childResults.add(mes.submit(new ExecutorCallable(childId, this
						.getChildren().get(childId), this.session)));
			}

			mes.shutdown();
			mes.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			
			for(Future<ExecutorIdentifier> fei : childResults) {
				ExecutorIdentifier ei = fei.get();
				
				childrenResults.put(ei.getId(), ei.getResult());
				
			}
			
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public ExecutableStatus getStatus() {
		return this.state;
	}

	@Override
	public Result getResults() throws ResourceInterfaceException {
		return this.action.getResults(this.session);
	}

	@Override
	public Action getAction() {
		return action;
	}

	/**
	 * Sets the action that is to be executed
	 * 
	 * @param action
	 *            Action
	 */
	public void setAction(Action action) {
		this.action = action;
	}

	/**
	 * Returns if the actions should be run synchronously
	 * 
	 * TRUE - Synchronously FALSE - Asynchronously
	 * 
	 * @return Blocking
	 */
	public boolean isBlocking() {
		return blocking;
	}

	/**
	 * Sets if the actions should be run synchronously
	 * 
	 * TRUE - Synchronously FALSE - Asynchronously
	 * 
	 * @param blocking
	 *            Blocking
	 */
	public void setBlocking(boolean blocking) {
		this.blocking = blocking;
	}

	/**
	 * Returns a map of children executables that are to be run
	 * 
	 * @return Children Executable
	 */
	public Map<String, Executable> getChildren() {
		return children;
	}

	/**
	 * Sets a map of children executables that are to be run
	 * 
	 * @param Children
	 *            Executable
	 */
	public void setChildren(Map<String, Executable> children) {
		this.children = children;
	}

	public void addChild(String name, Executable child) {
		child.setParent(this);
		this.children.put(name, child);
	}

	public void removeChild(String name) {
		Executable child = this.children.remove(name);
		child.setParent(null);
	}

	@Override
	public Executable getParent() {
		return parent;
	}

	@Override
	public void setParent(Executable parent) {
		this.parent = parent;
	}
}

class ExecutorCallable implements Callable<ExecutorIdentifier> {
	private String id;
	private SecureSession session;
	private Executable executable;

	public ExecutorCallable(String id, Executable executable,
			SecureSession session) {
		this.id = id;
		this.session = session;
		this.executable = executable;
	}

	@Override
	public ExecutorIdentifier call() throws Exception {
		Result result = new Result();

		executable.setup(this.session);
		try {
			executable.run();
			result = executable.getResults();
		} catch (ResourceInterfaceException e) {
			result.setResultStatus(ResultStatus.ERROR);
			result.setMessage(e.getMessage());
		}

		return new ExecutorIdentifier(this.id, result);
	}
}

class ExecutorIdentifier {
	private String id;
	private Result result;

	public ExecutorIdentifier(String id, Result result) {
		this.id = id;
		this.result = result;
	}

	public String getId() {
		return this.id;
	}

	public Result getResult() {
		return this.result;
	}
}