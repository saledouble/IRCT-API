/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.action;


import java.util.Date;
import java.util.Map;

import javax.naming.NamingException;

import edu.harvard.hms.dbmi.bd2k.irct.event.IRCTEventListener;
import edu.harvard.hms.dbmi.bd2k.irct.exception.ResourceInterfaceException;
import edu.harvard.hms.dbmi.bd2k.irct.model.process.IRCTProcess;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.Resource;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.implementation.ProcessResourceImplementationInterface;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Job;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.JobStatus;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.SecureSession;
import edu.harvard.hms.dbmi.bd2k.irct.util.Utilities;

/**
 * Implements the Action interface to run a process on a specific instance
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public class ProcessAction implements Action {
	
	private IRCTProcess process ;
	private Resource resource;
	private ActionStatus status;
	private Job job;
	
	private IRCTEventListener irctEventListener;
	
	/**
	 * Run a given process on a resource 
	 * 
	 * @param resource The resource to run the process on
	 * @param process The process to run
	 */
	public void setup(Resource resource, IRCTProcess process) {
		this.resource = resource;
		this.process = process;
		this.irctEventListener = Utilities.getIRCTEventListener();
	}
	
	@Override
	public void updateActionParams(Map<String, Job> updatedParams) {
		for(String key : updatedParams.keySet()) {
			process.getStringValues().put(key, updatedParams.get(key).getId().toString());
		}
	}
	
	@Override
	public void run(SecureSession session) {
		irctEventListener.beforeProcess(session, process);
		this.status = ActionStatus.RUNNING;
		try {
			ProcessResourceImplementationInterface processInterface = (ProcessResourceImplementationInterface) resource.getImplementingInterface();
			
			job = ActionUtilities.createJob(processInterface.getProcessDataType(process));
			if(session != null) {
				job.setUser(session.getUser());
			}
			
			process.setObjectValues(ActionUtilities.convertResultSetFieldToObject(session.getUser(), process.getProcessType().getFields(), process.getStringValues()));
			job = processInterface.runProcess(session, process, job);
			
			ActionUtilities.mergeResult(job);
		} catch (Exception e) {
			job.setMessage(e.getMessage());
			this.status = ActionStatus.ERROR;
		}
		irctEventListener.afterProcess(session, process);
	}

	@Override
	public Job getResults(SecureSession session) throws ResourceInterfaceException {
		this.job = ((ProcessResourceImplementationInterface)resource.getImplementingInterface()).getResults(session, job);
		try {
			while((this.job.getJobStatus() != JobStatus.ERROR) && (this.job.getJobStatus() != JobStatus.COMPLETE)) {
				Thread.sleep(5000);
				this.job = ((ProcessResourceImplementationInterface)resource.getImplementingInterface()).getResults(session, job);
			}
			
			job.getData().close();
		} catch(Exception e) {
			this.job.setJobStatus(JobStatus.ERROR);
			this.job.setMessage(e.getMessage());
		}
		
		
		job.setEndTime(new Date());
		//Save the query Action
		try {
			ActionUtilities.mergeResult(job);
			this.status = ActionStatus.COMPLETE;
		} catch (NamingException e) {
			job.setMessage(e.getMessage());
			this.status = ActionStatus.ERROR;
		}
		
		return this.job;
	}

	/**
	 * Get the process
	 * 
	 * @return Process
	 */
	public IRCTProcess getProcess() {
		return this.process;
	}

	/**
	 * Sets the process
	 * 
	 * @param process Process
	 */
	public void setProcess(IRCTProcess process) {
		this.process = process;
	}

	@Override
	public ActionStatus getStatus() {
		return status;
	}
	
	@Override
	public Resource getResource() {
		return this.resource;
	}
}
