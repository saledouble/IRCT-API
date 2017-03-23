/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.controller;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import javax.annotation.Resource;
import javax.ejb.Asynchronous;
import javax.ejb.Stateless;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.UserTransaction;

import edu.harvard.hms.dbmi.bd2k.irct.action.JoinAction;
import edu.harvard.hms.dbmi.bd2k.irct.action.ProcessAction;
import edu.harvard.hms.dbmi.bd2k.irct.action.QueryAction;
import edu.harvard.hms.dbmi.bd2k.irct.executable.Executable;
import edu.harvard.hms.dbmi.bd2k.irct.executable.ExecutableLeafNode;
import edu.harvard.hms.dbmi.bd2k.irct.executable.ExecutionPlan;
import edu.harvard.hms.dbmi.bd2k.irct.model.join.Join;
import edu.harvard.hms.dbmi.bd2k.irct.model.process.IRCTProcess;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.Query;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Persistable;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Job;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.JobStatus;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.exception.PersistableException;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.SecureSession;

/**
 * The execution controller is a stateless controller that manages the
 * executions of different processes, queries, and joins by creating an
 * execution plan and running it.
 * 
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
@Stateless
public class ExecutionController {

	@Inject
	Logger log;

	@PersistenceContext(unitName = "primary")
	EntityManager entityManager;

	@Resource(name = "DefaultManagedExecutorService")
	private ManagedExecutorService mes;
	
	@Inject
	private ResourceController rc;
	
	/**
	 * Runs the process
	 * 
	 * @param process
	 *            Process to run
	 * @param secureSession Session to run it in
	 * @return result id
	 * @throws PersistableException
	 *             Persistable exception occurred
	 */
	public Long runProcess(IRCTProcess process, SecureSession secureSession)
			throws PersistableException {
		Job newJob = new Job();
		newJob.setJobType("EXECUTION");
		if(secureSession != null) {
			newJob.setUser(secureSession.getUser());
		}

		newJob.setJobStatus(JobStatus.RUNNING);
		entityManager.persist(newJob);

		ProcessAction pa = new ProcessAction();
		pa.setup(process.getResources().get(0), process);

		ExecutableLeafNode eln = new ExecutableLeafNode();
		eln.setAction(pa);

		ExecutionPlan exp = new ExecutionPlan();
		exp.setup(eln, secureSession);

		runExecutionPlan(exp, newJob);

		return newJob.getId();
	}

	/**
	 * Run a query by creating an execution plan
	 * 
	 * @param query
	 *            Query
	 * @param secureSession Session to run it in
	 * @return Result Id
	 * @throws PersistableException
	 *             An error occurred
	 */
	public Long runQuery(Query query, SecureSession secureSession)
			throws PersistableException {
		Job newJob = new Job();
		newJob.setJobType("EXECUTION");
		if(secureSession != null) {
			newJob.setUser(secureSession.getUser());
		}

		newJob.setJobStatus(JobStatus.RUNNING);
		entityManager.persist(newJob);
		
		QueryAction qa = new QueryAction();
		edu.harvard.hms.dbmi.bd2k.irct.model.resource.Resource resource = (edu.harvard.hms.dbmi.bd2k.irct.model.resource.Resource) query.getResources().toArray()[0];
		if(!resource.isSetup()) {
			resource = rc.getResource(resource.getName());
		}
		qa.setup(resource, query);

		ExecutableLeafNode eln = new ExecutableLeafNode();
		eln.setAction(qa);

		ExecutionPlan exp = new ExecutionPlan();
		exp.setup(eln, secureSession);

		runExecutionPlan(exp, newJob);

		return newJob.getId();
	}

	/**
	 * Run a join by creating an execution plan
	 * 
	 * @param join
	 *            Join to run
	 * @param secureSession Session to run it in
	 * @return Result Id
	 * @throws PersistableException
	 *             An error occurred
	 */
	public Long runJoin(Join join, SecureSession secureSession)
			throws PersistableException {
		Job newJob = new Job();
		newJob.setJobType("EXECUTION");
		if(secureSession != null) {
			newJob.setUser(secureSession.getUser());
		}
		
		newJob.setJobStatus(JobStatus.RUNNING);
		entityManager.persist(newJob);
		
		JoinAction ja = new JoinAction();
		ja.setup(join);

		ExecutableLeafNode eln = new ExecutableLeafNode();
		eln.setAction(ja);

		ExecutionPlan exp = new ExecutionPlan();
		exp.setup(eln, secureSession);
		runExecutionPlan(exp, newJob);

		return newJob.getId();
	}
	
	
	public Long runExecutable(Executable executable, SecureSession secureSession) throws PersistableException {
		Job newJoin = new Job();
		newJoin.setJobType("EXECUTION");
		if(secureSession != null) {
			newJoin.setUser(secureSession.getUser());
		}
		
		newJoin.setJobStatus(JobStatus.RUNNING);
		entityManager.persist(newJoin);
		
		ExecutionPlan exp = new ExecutionPlan();
		exp.setup(executable, secureSession);
		runExecutionPlan(exp, newJoin);

		return newJoin.getId();
		
	}

	/**
	 * Runs an execution plan
	 * 
	 * @param executionPlan
	 *            Execution Plan
	 * @param job
	 *            Job
	 * @throws PersistableException
	 *             A persistable exception occurred
	 */
	@Asynchronous
	public void runExecutionPlan(final ExecutionPlan executionPlan,
			final Job job) throws PersistableException {

		Callable<Job> runPlan = new Callable<Job>() {
			@Override
			public Job call() {
				try {
					job.setStartTime(new Date());
					executionPlan.run();
					
					Job finalResult = executionPlan.getResults();
					
					if ((finalResult.getJobStatus() == JobStatus.COMPLETE) && (finalResult.getData() instanceof Persistable)) {
						job.setDataType(finalResult.getDataType());
						job.setData(finalResult.getData());
						job.setResultSetLocation(finalResult.getResultSetLocation());
						job.setMessage(finalResult.getMessage());
						
						if(((Persistable) job.getData()).isPersisted()) {
							((Persistable) job.getData()).merge();
						} else {
							((Persistable) job.getData()).persist();
						}
						job.setJobStatus(JobStatus.AVAILABLE);
					} else {
						job.setJobStatus(JobStatus.ERROR);
						job.setMessage(finalResult.getMessage());
					}
					
					job.setEndTime(new Date());
					UserTransaction userTransaction = lookup();
					userTransaction.begin();
					entityManager.merge(job);
					userTransaction.commit();
				} catch (PersistableException e) {
					job.setJobStatus(JobStatus.ERROR);
					job.setMessage(e.getMessage());
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
					log.info(e.getMessage());
					job.setJobStatus(JobStatus.ERROR);
				} finally {
					
				}
				return job;
			}
		};

		mes.submit(runPlan);
	}

	private UserTransaction lookup() throws NamingException {
		InitialContext ic = new InitialContext();
		return (UserTransaction) ic.lookup("java:comp/UserTransaction");
	}
}
