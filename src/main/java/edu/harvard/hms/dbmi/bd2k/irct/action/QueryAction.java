/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.action;

import java.util.Date;
import java.util.Map;

import javax.naming.NamingException;

import edu.harvard.hms.dbmi.bd2k.irct.model.query.ClauseAbstract;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.Query;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.WhereClause;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.Resource;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.implementation.QueryResourceImplementationInterface;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Persistable;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Job;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.JobStatus;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.SecureSession;
import edu.harvard.hms.dbmi.bd2k.irct.util.Utilities;
import edu.harvard.hms.dbmi.bd2k.irct.event.IRCTEventListener;
import edu.harvard.hms.dbmi.bd2k.irct.exception.ResourceInterfaceException;

/**
 * Implements the Action interface to run a query on a specific instance
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public class QueryAction implements Action {

	private Query query;
	private Resource resource;
	private ActionStatus status;
	private Job job;

	private IRCTEventListener irctEventListener;

	/**
	 * Sets up the action to run a given query on a resource
	 * 
	 * @param resource
	 *            Resource to run the query
	 * @param query
	 *            Run the query
	 */
	public void setup(Resource resource, Query query) {
		this.query = query;
		this.resource = resource;
		this.status = ActionStatus.CREATED;
		this.irctEventListener = Utilities.getIRCTEventListener();
	}

	@Override
	public void updateActionParams(Map<String, Job> updatedParams) {
		for (String key : updatedParams.keySet()) {
			Long clauseId = Long.valueOf(key.split(".")[0]);
			String parameterId = key.split(".")[1];

			ClauseAbstract clause = this.query.getClauses().get(clauseId);
			if (clause instanceof WhereClause) {
				WhereClause whereClause = (WhereClause) clause;
				whereClause.getStringValues().put(parameterId,
						updatedParams.get(key).getId().toString());
			}
		}
	}

	@Override
	public void run(SecureSession session) {
		irctEventListener.beforeQuery(session, resource, query);
		this.status = ActionStatus.RUNNING;
		try {
			QueryResourceImplementationInterface queryInterface = (QueryResourceImplementationInterface) resource
					.getImplementingInterface();

			this.job = ActionUtilities.createJob(queryInterface
					.getQueryDataType(query));

			if (session != null) {
				this.job.setUser(session.getUser());
			}
			
			

			this.job = queryInterface.runQuery(session, query, job);

			// Update the result in the database
			ActionUtilities.mergeResult(this.job);
		} catch (Exception e) {
			this.job.setJobStatus(JobStatus.ERROR);
			this.job.setMessage(e.getMessage());
			this.status = ActionStatus.ERROR;
		}
		irctEventListener.afterQuery(session, resource, query);
	}

	@Override
	public Job getResults(SecureSession session)
			throws ResourceInterfaceException {
		try {
			this.job = ((QueryResourceImplementationInterface) resource
					.getImplementingInterface()).getResults(session, job);

			while ((this.job.getJobStatus() != JobStatus.ERROR)
					&& (this.job.getJobStatus() != JobStatus.COMPLETE)) {
				Thread.sleep(3000);
				this.job = ((QueryResourceImplementationInterface) resource
						.getImplementingInterface())
						.getResults(session, job);
			}

			if (this.job.getJobStatus() == JobStatus.COMPLETE) {
				if (((Persistable) job.getData()).isPersisted()) {
					((Persistable) job.getData()).merge();
				} else {
					((Persistable) job.getData()).persist();
				}

			}

			job.getData().close();
		} catch (Exception e) {
			this.job.setJobStatus(JobStatus.ERROR);
			this.job.setMessage(e.getMessage());
		}

		job.setEndTime(new Date());
		// Save the query Action
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
	 * Returns the query
	 * 
	 * @return Query
	 */
	public Query getQuery() {
		return this.query;

	}

	/**
	 * Sets the query
	 * 
	 * @param query
	 *            Query
	 */
	public void setQuery(Query query) {
		this.query = query;
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
