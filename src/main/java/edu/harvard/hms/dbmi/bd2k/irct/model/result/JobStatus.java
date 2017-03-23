/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.model.result;

/**
 * An enum representing all the different states a job can be in.
 * 
 * CREATED - The job has been created but not run
 * RUNNING - The job is currently running but has not completed
 * COMPLETE - The job is ready
 * AVAILABLE - The job is available to the user
 * ERROR - An error occurred
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public enum JobStatus {
	CREATED, RUNNING, AVAILABLE, COMPLETE, ERROR;
}