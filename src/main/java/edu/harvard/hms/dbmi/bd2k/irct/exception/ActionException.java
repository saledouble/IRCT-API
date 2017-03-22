package edu.harvard.hms.dbmi.bd2k.irct.exception;

/**
 * Indicates an Action Exception occrred of some type
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
public class ActionException extends Exception {
	private static final long serialVersionUID = -6859276764441640509L;

	/**
	 * An exception occurred setting up an action
	 * 
	 * @param message Message
	 */
	public ActionException(String message) {
		super(message);
	}
}
