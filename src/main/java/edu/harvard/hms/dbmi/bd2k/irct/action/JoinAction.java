/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.action;

import java.util.HashMap;
import java.util.Map;

import javax.naming.NamingException;

import edu.harvard.hms.dbmi.bd2k.irct.model.join.Join;
import edu.harvard.hms.dbmi.bd2k.irct.model.join.JoinImplementation;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.Resource;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Job;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.JobStatus;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.exception.PersistableException;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.exception.ResultSetException;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.SecureSession;
import edu.harvard.hms.dbmi.bd2k.irct.util.Utilities;
import edu.harvard.hms.dbmi.bd2k.irct.event.IRCTEventListener;
import edu.harvard.hms.dbmi.bd2k.irct.exception.JoinActionSetupException;
import edu.harvard.hms.dbmi.bd2k.irct.exception.ResourceInterfaceException;

/**
 * Implements the Action interface to run a join
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public class JoinAction implements Action {
	private Join join;
	private ActionStatus status;
	private Resource resource;
	private Job job;
	
	private IRCTEventListener irctEventListener;

	/**
	 * Sets up the IRCT Join Action
	 * 
	 * @param join The join to run
	 */
	public void setup(Join join) {
		this.status = ActionStatus.CREATED;
		this.join = join;
		this.irctEventListener = Utilities.getIRCTEventListener();
		this.resource = null;
	}
	
	@Override
	public void updateActionParams(Map<String, Job> updatedParams) {
		for(String key : this.join.getStringValues().keySet()) {
			String value = this.join.getStringValues().get(key);
			if(updatedParams.containsKey(value)) {
				this.join.getStringValues().put(key,  updatedParams.get(value).getId().toString());
			}
		}
	}
	
	@Override
	public void run(SecureSession session) {
		irctEventListener.beforeJoin(session, join);
		this.status = ActionStatus.RUNNING;

		try {
			JoinImplementation joinImplementation = (JoinImplementation) join.getJoinImplementation();
			joinImplementation.setup(new HashMap<String, Object>());
			job = ActionUtilities.createJob(joinImplementation.getJoinDataType());
			if(session != null) {
				job.setUser(session.getUser());
			}
			
			join.getObjectValues().putAll(ActionUtilities.convertResultSetFieldToObject(session.getUser(), join.getJoinType().getFields(), join.getStringValues()));
			
			job = joinImplementation.run(session, join, job);
			this.status = ActionStatus.COMPLETE;
			ActionUtilities.mergeResult(job);
		} catch (PersistableException | NamingException | ResultSetException | JoinActionSetupException e) {
			job.setMessage(e.getMessage());
			this.status = ActionStatus.ERROR;
		}
		
		this.status = ActionStatus.COMPLETE;
		irctEventListener.afterJoin(session, join);
	}

	@Override
	public Job getResults(SecureSession session) throws ResourceInterfaceException {
		if(this.job.getJobStatus() != JobStatus.ERROR && this.job.getJobStatus() != JobStatus.COMPLETE) {
			this.job = this.join.getJoinImplementation().getResults(this.job);
		}
		try {
			ActionUtilities.mergeResult(job);
			this.status = ActionStatus.COMPLETE;
		} catch (NamingException e) {
			job.setMessage(e.getMessage());
			this.status = ActionStatus.ERROR;
		}
		return this.job;
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
