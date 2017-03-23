/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.model.result;

/**
 * An enum representing the different types of data that is associated with a job
 * 
 * TABULAR - Data that is in grid format
 * JSON - Data that is in JSON format
 * HTML - Data that is HTML
 * IMAGE - Data that is an image
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public enum JobDataType {
	TABULAR, JSON, HTML, IMAGE
}
