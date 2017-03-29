package edu.harvard.hms.dbmi.bd2k.irct.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.ejb.Stateful;
import javax.inject.Inject;

import edu.harvard.hms.dbmi.bd2k.irct.action.JoinAction;
import edu.harvard.hms.dbmi.bd2k.irct.action.QueryAction;
import edu.harvard.hms.dbmi.bd2k.irct.exception.ActionException;
import edu.harvard.hms.dbmi.bd2k.irct.exception.QueryException;
import edu.harvard.hms.dbmi.bd2k.irct.executable.Executable;
import edu.harvard.hms.dbmi.bd2k.irct.executable.ExecutableChildNode;
import edu.harvard.hms.dbmi.bd2k.irct.model.join.IRCTJoin;
import edu.harvard.hms.dbmi.bd2k.irct.model.join.Join;
import edu.harvard.hms.dbmi.bd2k.irct.model.ontology.DataType;
import edu.harvard.hms.dbmi.bd2k.irct.model.ontology.Entity;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.ClauseAbstract;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.JoinClause;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.JoinType;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.PredicateType;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.Query;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.SelectClause;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.SelectOperationType;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.SortClause;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.SortOperationType;
import edu.harvard.hms.dbmi.bd2k.irct.model.query.WhereClause;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.Field;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.LogicalOperator;
import edu.harvard.hms.dbmi.bd2k.irct.model.resource.Resource;

/**
 * A stateful controller for creating a actions
 * 
 * @author Jeremy R. Easton-Marks
 *
 */
@Stateful
public class ActionController {

	@Inject
	Logger log;

	@Inject
	private ResourceController rc;

	private Map<Resource, Executable> orphans;

	private Map<Resource, Executable> missing;

	private ExecutableChildNode rootPosition;

	/**
	 * Sets up the controller to create a new execution tree. This must be run
	 * before any other actions are added.
	 * 
	 * @return Root Executable Child Node
	 */
	public ExecutableChildNode createAction() {
		this.orphans = new HashMap<Resource, Executable>();
		this.missing = new HashMap<Resource, Executable>();
		this.rootPosition = new ExecutableChildNode();
		return this.rootPosition;
	}

	/**
	 * Adds a select clause to the base position, or child executable position
	 * in the execution tree.
	 * 
	 * @param basePosition
	 *            Position in execution tree that is current
	 * @param field
	 *            Field to be selected upon
	 * @param alias
	 *            Alias of that field, if applicable
	 * @param operation
	 *            Operation to be run if applicable
	 * @param fields
	 *            String representation of the fields for the select operation
	 *            in key : value form
	 * @param objectFields
	 *            Objects of the fields for the select operation in key : value
	 *            form
	 * @throws QueryException
	 *             An exception occurred adding the select to the query
	 * @throws ActionException
	 *             An exception occurred creating the action
	 */
	public void addSelectClause(ExecutableChildNode basePosition, Entity field,
			String alias, SelectOperationType operation,
			Map<String, String> fields, Map<String, Object> objectFields)
			throws QueryException, ActionException {

		Resource resource = getResource(field.getPui());
		validateSelectClause(resource, operation, fields);

		// Create the select clause
		SelectClause selectClause = new SelectClause();
		selectClause.setParameters(field);
		selectClause.setAlias(alias);
		selectClause.setOperationType(operation);
		selectClause.setStringValues(fields);
		selectClause.setObjectValues(objectFields);

		addClause(resource, selectClause, basePosition);
	}

	/**
	 * Adds a where clause to the base position, or child executable position
	 * relative to the base position in the execution tree.
	 * 
	 * @param basePosition
	 *            Position in execution tree that is current
	 * @param field
	 *            Field for the predicate to be run against
	 * @param predicate
	 *            Predicate for the where clause
	 * @param logicalOperator
	 *            Logical operator for multiple clauses
	 * @param fields
	 *            A string representation of the fields for the where clause in
	 *            key : value form
	 * @param objectFields
	 *            Objects of the fields for the where clause in key : value form
	 * @throws ActionException
	 *             An exception occurred creating the action
	 * @throws QueryException
	 *             An exception occurred adding the where clause to the query
	 */
	public void addWhereClause(ExecutableChildNode basePosition, Entity field,
			PredicateType predicate, LogicalOperator logicalOperator,
			Map<String, String> fields, Map<String, Object> objectFields)
			throws ActionException, QueryException {
		Resource resource = getResource(field.getPui());
		// Is valid where clause
		validateWhereClause(resource, field, predicate, logicalOperator, fields);

		// Create the where Clause
		WhereClause whereClause = new WhereClause();
		whereClause.setField(field);
		whereClause.setLogicalOperator(logicalOperator);
		whereClause.setPredicateType(predicate);
		whereClause.setStringValues(fields);
		whereClause.setObjectValues(objectFields);

		addClause(resource, whereClause, basePosition);

	}

	/**
	 * Adds a sort clause to the base position, or child executable position
	 * relative to the base position in the execution tree.
	 * 
	 * @param basePosition
	 *            Position in execution tree that is current
	 * @param field
	 *            Field for the sort to occur on
	 * @param operation
	 *            Sort Operation
	 * @param fields
	 *            A string representation of the fields for the sort operation
	 *            in key : value form
	 * @param objectFields
	 *            Objects for the sort operation in key : value form
	 * @throws ActionException
	 *             An exception occurred creating the action
	 * @throws QueryException
	 *             An exception occurred adding the sort clause to the query
	 */
	public void addSortClause(ExecutableChildNode basePosition, Entity field,
			SortOperationType operation, Map<String, String> fields,
			Map<String, Object> objectFields) throws ActionException,
			QueryException {

		Resource resource = getResource(field.getPui());
		validateSortClause(resource, operation, fields);

		// Create the sort clause
		SortClause sortClause = new SortClause();
		sortClause.setParameters(field);
		sortClause.setStringValues(fields);
		sortClause.setOperationType(operation);
		sortClause.setObjectValues(objectFields);

		addClause(resource, sortClause, basePosition);
	}

	/**
	 * Adds a join clause to the base position, or child executable position
	 * relative to the base position in the execution tree.
	 * 
	 * @param basePosition
	 *            Position in execution tree that is current
	 * @param field
	 *            Field for the join to occur on
	 * @param joinType
	 *            Type of join to be performed
	 * @param fields
	 *            A string representation of the fields for the join operation
	 * @param objectFields
	 *            A map of objects for the join operation
	 * @throws ActionException
	 *             An exception occurred creating the action
	 * @throws QueryException
	 *             An exception occurred adding the join clause to the query
	 */
	public void addJoinClause(ExecutableChildNode basePosition, Entity field,
			JoinType joinType, Map<String, String> fields,
			Map<String, Object> objectFields) throws ActionException,
			QueryException {

		Resource resource = getResource(field.getPui());
		validateJoinClause(resource, joinType, fields, objectFields);

		// Create the sort clause
		JoinClause joinClause = new JoinClause();
		joinClause.setStringValues(fields);
		joinClause.setJoinType(joinType);
		joinClause.setObjectValues(objectFields);

		addClause(resource, joinClause, basePosition);
	}

	/**
	 * Creates an IRCT join that will combine the results of two actions
	 * relative to the base position
	 * 
	 * @param basePosition
	 *            Position in the execution tree that is current
	 * @param irctJoin
	 *            IRCT Join to be performed
	 * @param fields
	 *            String representation of the fields for the join operation
	 * @param objectFields
	 *            A map of objects for performing the join operation
	 * @throws ActionException
	 *             An exception occurred adding the IRCT join
	 */
	public void addJoinClause(ExecutableChildNode basePosition,
			IRCTJoin irctJoin, Map<String, String> fields,
			Map<String, Object> objectFields) throws ActionException {
		Join join = new Join();

		join.setJoinImplementation(irctJoin.getJoinImplementation());
		join.setJoinType(irctJoin);
		join.setStringValues(fields);
		join.setObjectValues(objectFields);

		addJoin(join, rootPosition);
	}

	/**
	 * Validates the execution tree to ensure that no orphan executable remain,
	 * and that all expected actions are accounted for
	 * 
	 * @throws ActionException
	 *             An exception occurred validating the action
	 */
	public void validateAction() throws ActionException {
		if (!this.orphans.isEmpty()) {
			throw new ActionException(
					"Resource "
							+ this.orphans.keySet().toArray(new Resource[0])[0]
									.getName() + " is not referenced");
		}
		if (!this.missing.isEmpty()) {
			throw new ActionException(
					"Cannot find expected resource "
							+ this.missing.keySet().toArray(new Resource[0])[0]
									.getName());
		}
	}

	/**
	 * Validates the executable and returns the root position in the executable
	 * tree
	 * 
	 * @return Root Executable
	 * @throws ActionException
	 *             An exception occurred validating the execution tree
	 */
	public ExecutableChildNode getRootExecutionNode() throws ActionException {
		validateAction();
		return this.rootPosition;
	}

	/**
	 * Deletes the current action
	 * 
	 */
	public void deleteAction() {
		this.orphans = null;
		this.rootPosition = null;
	}

	private void addClause(Resource resource, ClauseAbstract clause,
			ExecutableChildNode baseExecutable) {
		ExecutableChildNode workingExecutable = getExecutable(resource,
				(ExecutableChildNode) baseExecutable);

		// No executable
		if (workingExecutable.getAction() == null) {
			Query query = new Query();
			QueryAction queryAction = new QueryAction();
			queryAction.setup(resource, query);
			workingExecutable.setAction(queryAction);
		}

		((QueryAction) workingExecutable.getAction()).getQuery().addClause(
				clause);

		addExecutable(workingExecutable, baseExecutable, resource);
	}

	private void addJoin(Join join, ExecutableChildNode baseExecutable)
			throws ActionException {
		JoinAction joinAction = new JoinAction();
		joinAction.setup(join);

		ExecutableChildNode joinExecutable = new ExecutableChildNode();
		joinExecutable.setAction(joinAction);

		addJoinExecutable(joinExecutable, baseExecutable, join, "Left");
		addJoinExecutable(joinExecutable, baseExecutable, join, "Right");

	}

	private void addJoinExecutable(ExecutableChildNode joinExecutable,
			ExecutableChildNode baseExecutable, Join join, String field)
			throws ActionException {
		String pui = join.getStringValues().remove(field);
		Resource resource = getResource(pui);

		join.getStringValues().put(field + "ResultSet", resource.getName());
		join.getStringValues().put(field + "Column",
				pui.replaceAll("/" + resource.getName() + "/", ""));

		SelectClause selectClause = new SelectClause();
		selectClause.setParameters(new Entity(pui));

		if (baseExecutable.getAction().getResource() == resource) {

			if (baseExecutable.getAction() instanceof QueryAction) {
				((QueryAction) baseExecutable.getAction()).getQuery()
						.addClause(selectClause);
			}

			joinExecutable.addChild(resource.getName(), baseExecutable);

			if (baseExecutable == rootPosition) {
				rootPosition = joinExecutable;
			}
		} else if (orphans.containsKey(resource)) {
			Executable orphan = orphans.remove(resource);

			if (orphan.getAction() instanceof QueryAction) {
				((QueryAction) orphan.getAction()).getQuery().addClause(
						selectClause);
			}

			joinExecutable.addChild(resource.getName(), orphan);
		}

	}

	private void addExecutable(ExecutableChildNode workingExecutable,
			ExecutableChildNode baseExecutable, Resource resource) {
		// If first pass then add it as the root node
		if (rootPosition == null) {
			// If first pass then add it as the root node
			rootPosition = workingExecutable;
		} else if (baseExecutable == null) {
			orphans.put(resource, workingExecutable);
		} else if (baseExecutable != workingExecutable
				&& !baseExecutable.getChildren().containsKey(resource)
				&& !orphans.containsKey(resource)) {
			// If not a child then of the base position then add it as an orphan
			orphans.put(resource, workingExecutable);
		}
	}

	private ExecutableChildNode getExecutable(Resource resource,
			ExecutableChildNode position) {
		ExecutableChildNode returns = new ExecutableChildNode();
		if (position.getAction() == null) {
			return position;
		}

		if (position.getAction().getResource() == resource) {
			returns = position;
		}

		for (Executable children : position.getChildren().values()) {
			if (children instanceof ExecutableChildNode) {
				returns = getExecutable(resource, position);

				if (returns != null) {
					break;
				}
			}
		}

		// Check the orphans
		if (returns == null) {
			returns = (ExecutableChildNode) orphans.get(resource);
		}

		return returns;
	}

	private Resource getResource(String pui) throws ActionException {
		if (pui == null || pui.isEmpty()) {
			throw new ActionException("Invalid Path: " + pui);
		}
		Resource resource = rc.getResource(pui.split("/")[1]);
		if (resource == null) {
			throw new ActionException("Unknown Resource: " + pui.split("/")[1]);
		}
		return resource;
	}

	private void validateSelectClause(Resource resource,
			SelectOperationType operation, Map<String, String> selectFields)
			throws QueryException {

		// Is the select operation supported by the resource
		if (operation != null) {
			if (!resource.getSupportedSelectOperations().contains(operation)) {
				throw new QueryException(
						"Select operation is not supported by the resource");
			}
			// Are all the fields valid?
			validateFields(operation.getFields(), selectFields, null);
		}

	}

	private void validateWhereClause(Resource resource, Entity field,
			PredicateType predicate, LogicalOperator logicalOperator,
			Map<String, String> queryFields) throws QueryException {
		// Does the resource support the logical operator
		if ((logicalOperator != null)
				&& (!resource.getLogicalOperators().contains(logicalOperator))) {
			throw new QueryException("Logical operator " + logicalOperator
					+ " is not supported by the resource");
		}
		// Does the resource support the predicate?
		if (!resource.getSupportedPredicates().contains(predicate)) {
			throw new QueryException("Predicate " + predicate
					+ "is not supported by the resource");
		}
		// Does the predicate support the entity?
		if ((!predicate.getDataTypes().isEmpty())
				&& (!predicate.getDataTypes().contains(field.getDataType()))) {
			throw new QueryException(
					"Predicate does not support this type of field");
		}
		// Are all the fields valid?
		validateFields(predicate.getFields(), queryFields, null);
	}

	private void validateFields(List<Field> fields,
			Map<String, String> valueFields, Map<String, Object> objectFields)
			throws QueryException {

		for (Field predicateField : fields) {

			if (predicateField.isRequired()
					&& ((valueFields != null) && (valueFields
							.containsKey(predicateField.getPath())))) {
				String queryFieldValue = valueFields.get(predicateField
						.getPath());

				if (queryFieldValue != null) {
					// Is the predicate field data type allowed for this query
					// field
					if (!predicateField.getDataTypes().isEmpty()) {
						boolean validateFieldValue = false;

						for (DataType dt : predicateField.getDataTypes()) {
							if (dt.validate(queryFieldValue)) {
								validateFieldValue = true;
								break;
							}
						}

						if (!validateFieldValue) {
							throw new QueryException(
									"The field value set is not a supported type for this field");
						}
					}
					// Is the predicate field of allowedTypes
					if (!predicateField.getPermittedValues().isEmpty()
							&& (!predicateField.getPermittedValues().contains(
									queryFieldValue))) {
						throw new QueryException(
								"The field value is not of an allowed type");
					}
				}

			} else if (predicateField.isRequired()
					&& ((objectFields != null) && (objectFields
							.containsKey(predicateField.getPath())))) {

			} else if (predicateField.isRequired()) {
				throw new QueryException("Required field "
						+ predicateField.getName() + " is not set");
			}
		}
	}

	private void validateSortClause(Resource resource,
			SortOperationType operation, Map<String, String> sortFields)
			throws QueryException {
		// Is the sort operation supported by the resource
		if ((operation != null)
				&& (!resource.getSupportedSortOperations().contains(operation))) {
			throw new QueryException(
					"Sort operation is not supported by the resource");
		}

		// Are all the fields valid?
		validateFields(operation.getFields(), sortFields, null);
	}

	private void validateJoinClause(Resource resource, JoinType joinType,
			Map<String, String> joinFields, Map<String, Object> objectFields)
			throws QueryException {

		// Does the resource support the join type
		if (!resource.getSupportedJoins().contains(joinType)) {
			throw new QueryException(
					"Join Type is not supported by the resource");
		}

		// Are all the fields valid?
		validateFields(joinType.getFields(), joinFields, objectFields);
	}

}
