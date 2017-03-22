/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package edu.harvard.hms.dbmi.bd2k.irct.executable;

import edu.harvard.hms.dbmi.bd2k.irct.model.result.Result;
import edu.harvard.hms.dbmi.bd2k.irct.model.security.SecureSession;
import edu.harvard.hms.dbmi.bd2k.irct.action.Action;
import edu.harvard.hms.dbmi.bd2k.irct.event.IRCTEventListener;
import edu.harvard.hms.dbmi.bd2k.irct.exception.ResourceInterfaceException;

/**
 * The executable interface provides a set of methods that an executable must
 * implement. An executable is the implementing class for any type of process,
 * query, or action.
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public interface Executable {
	void setup(SecureSession secureSession);

	/**
	 * Run the executable
	 * 
	 * @throws ResourceInterfaceException An error occurred
	 */
	void run() throws ResourceInterfaceException;

	/**
	 * Get the current state of the executable
	 * 
	 * @return Executable State
	 */
	ExecutableStatus getStatus();

	/**
	 * Returns the result set
	 * 
	 * @return Result
	 * @throws ResourceInterfaceException An error occurred
	 */
	Result getResults() throws ResourceInterfaceException;
	
	/**
	 * Returns the action that is to be executed
	 * 
	 * @return Action
	 */
	Action getAction(); 
	
	
	/**
	 * Returns the parent of the executable
	 * 
	 * @return Parent Executable
	 */
	Executable getParent();
	
	/**
	 * Sets the parent of the executable
	 * 
	 * @param parent Parent
	 */
	void setParent(Executable parent);
}
