/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import edu.harvard.hms.dbmi.bd2k.irct.controller.ResultController;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.Field;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.PrimitiveDataType;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.Job;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.JobDataType;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.exception.PersistableException;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.exception.ResultSetException;
import edu.harvard.hms.dbmi.bd2k.irct.model.result.tabular.ResultSet;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.User;

/**
 * A set of utitlity functions that can be used by the different implementation of actions
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public class ActionUtilities {
	
	/**
	 * Creates a job with a given different result data type
	 * 
	 * @param resultDataType
	 * @return The new job
	 * @throws NamingException An exception occurred getting the result controller
	 * @throws PersistableException An error occurred saving the result.
	 */ 
	static protected Job createJob(JobDataType resultDataType) throws NamingException, PersistableException {
		InitialContext ic = new InitialContext();
		ResultController resultController = (ResultController) ic.lookup("java:global/IRCT-CL/ResultController");
		Job job = resultController.createResult(resultDataType); 
		job.setJobType("ACTION");
		return job;
	}
	
	/**
	 * Saves the results 
	 * 
	 * @param job Job to serve
	 * @throws NamingException An exception occurred getting the result controller
	 */
	static protected void mergeResult(Job job) throws NamingException {
		InitialContext ic = new InitialContext();
		ResultController resultController = (ResultController) ic.lookup("java:global/IRCT-CL/ResultController");
		resultController.mergeResult(job);
	}
	
	/**
	 * Returns an array of 
	 * 
	 * @param user User
	 * @param fields Fields
	 * @param stringValues String values
	 * @return A map of field ids, and results
	 * @throws NamingException An exception occurred getting the result controller
	 * @throws ResultSetException An occurred getting the result
	 * @throws PersistableException An error occurred saving the result.
	 */
	static protected Map<String, Object> convertResultSetFieldToObject(User user, List<Field> fields, Map<String, String> stringValues) throws NamingException, ResultSetException, PersistableException {
		Map<String, Object> returns = new HashMap<String, Object>();
		InitialContext ic = new InitialContext();
		ResultController resultController = (ResultController) ic.lookup("java:global/IRCT-CL/ResultController");
		
		for(Field field : fields) {
			if(field.getDataTypes().contains(PrimitiveDataType.RESULTSET)) {
				
				Job result = resultController.getResult(user, Long.valueOf(stringValues.get(field.getPath())));
				ResultSet rs = (ResultSet) result.getData();
				rs.load(result.getResultSetLocation());
				returns.put(field.getPath(), rs);
			}
		}
		
		return returns;
	}
}
