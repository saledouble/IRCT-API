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
import edu.harvard.hms.dbmi.bd2k.irct.model.process.IRCTProcess;
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

	public void createAction() {
		this.orphans = new HashMap<Resource, Executable>();
		this.missing = new HashMap<Resource, Executable>();
		this.rootPosition = null;
	}

	public void addQuery(Query query) {

	}

	public void addSelectClause(Entity field, String alias,
			SelectOperationType operation, Map<String, String> fields,
			Map<String, Object> objectFields) throws QueryException,
			ActionException {
		addSelectClause(field, alias, operation, fields, objectFields,
				rootPosition);
	}

	public void addSelectClause(Entity field, String alias,
			SelectOperationType operation, Map<String, String> fields,
			Map<String, Object> objectFields, ExecutableChildNode basePosition)
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

	public void addWhereClause(Entity field, PredicateType predicate,
			LogicalOperator logicalOperator, Map<String, String> fields,
			Map<String, Object> objectFields) throws ActionException,
			QueryException {
		addWhereClause(field, predicate, logicalOperator, fields, objectFields,
				rootPosition);
	}

	public void addWhereClause(Entity field, PredicateType predicate,
			LogicalOperator logicalOperator, Map<String, String> fields,
			Map<String, Object> objectFields, ExecutableChildNode basePosition)
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

	public void addSortClause(Entity field, SortOperationType operation,
			Map<String, String> fields, Map<String, Object> objectFields)
			throws ActionException, QueryException {
		addSortClause(field, operation, fields, objectFields, rootPosition);
	}

	public void addSortClause(Entity field, SortOperationType operation,
			Map<String, String> fields, Map<String, Object> objectFields,
			ExecutableChildNode basePosition) throws ActionException,
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

	

	public void addJoinClause(Entity field, JoinType joinType,
			Map<String, String> fields, Map<String, Object> objectFields)
			throws ActionException, QueryException {
		addJoinClause(field, joinType, fields, objectFields, rootPosition);
	}

	public void addJoinClause(Entity field, JoinType joinType,
			Map<String, String> fields, Map<String, Object> objectFields,
			ExecutableChildNode basePosition) throws ActionException,
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
	
	public void addJoinClause(Entity entity, IRCTJoin irctJoin,
			Map<String, String> fields, Map<String, Object> objectFields) throws QueryException, ActionException {
		addJoinClause(entity, irctJoin, fields, objectFields, rootPosition);
	}

	
	public void addJoinClause(Entity entity, IRCTJoin irctJoin,
			Map<String, String> fields, Map<String, Object> objectFields, ExecutableChildNode basePosition) throws QueryException, ActionException {
		Join join = new Join();
		
		join.setJoinImplementation(irctJoin.getJoinImplementation());
		join.setJoinType(irctJoin);
		join.setStringValues(fields);
		join.setObjectValues(objectFields);
		
		addJoin(join, rootPosition);
	}
	
	public void addProcess(IRCTProcess process) {
	}


	public void validateAction() throws ActionException {
		if(!this.orphans.isEmpty()) {
			throw new ActionException("Resource " + this.orphans.keySet().toArray(new Resource[0])[0].getName() + " is not referenced");
		}
		if(!this.missing.isEmpty()) {
			throw new ActionException("Cannot find expected resource " + this.missing.keySet().toArray(new Resource[0])[0].getName());
		}
	}
	
	public ExecutableChildNode getRootExecutionNode() throws ActionException {
		validateAction();
		return this.rootPosition;
	}

	public void deleteAction() {
		this.orphans = null;
		this.rootPosition = null;
	}

	private void addClause(Resource resource, ClauseAbstract clause,
			ExecutableChildNode baseExecutable) {
		ExecutableChildNode workingExecutable = getExecutable(resource,
				(ExecutableChildNode) baseExecutable);

		// No executable
		if (workingExecutable == null) {
			Query query = new Query();
			QueryAction queryAction = new QueryAction();
			queryAction.setup(resource, query);
			workingExecutable = new ExecutableChildNode();
			workingExecutable.setAction(queryAction);
		}

		((QueryAction) workingExecutable.getAction()).getQuery().addClause(
				clause);

		addExecutable(workingExecutable, baseExecutable, resource);
	}
	
	private void addJoin(Join join, ExecutableChildNode baseExecutable) throws ActionException {
		JoinAction joinAction = new JoinAction();
		joinAction.setup(join);
		
		ExecutableChildNode joinExecutable  = new ExecutableChildNode();
		joinExecutable.setAction(joinAction);
		
		addJoinExecutable(joinExecutable, baseExecutable, join, "Left");
		addJoinExecutable(joinExecutable, baseExecutable, join, "Right");
		
	}
	
	private void addJoinExecutable(ExecutableChildNode joinExecutable, ExecutableChildNode baseExecutable, Join join, String field) throws ActionException {
		String pui = join.getStringValues().remove(field);
		Resource resource = getResource(pui);
		
		join.getStringValues().put(field + "ResultSet", resource.getName());
		join.getStringValues().put(field + "Column", pui.replaceAll("/" + resource.getName() + "/", ""));
		
		SelectClause selectClause = new SelectClause();
		selectClause.setParameters(new Entity(pui));
		
		if(baseExecutable.getAction().getResource() == resource) {
			
			if(baseExecutable.getAction() instanceof QueryAction) {
				((QueryAction) baseExecutable.getAction()).getQuery().addClause(selectClause);
			}
			
			joinExecutable.addChild(resource.getName(), baseExecutable);
			
			if(baseExecutable == rootPosition) {
				rootPosition = joinExecutable;
			}
		} else if(orphans.containsKey(resource)){
			Executable orphan = orphans.remove(resource);
			
			if(orphan.getAction() instanceof QueryAction) {
				((QueryAction) orphan.getAction()).getQuery().addClause(selectClause);
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
		} else if (baseExecutable != workingExecutable
				&& !baseExecutable.getChildren().containsKey(resource)
				&& !orphans.containsKey(resource)) {
			// If not a child then of the base position then add it as an orphan
			orphans.put(resource, workingExecutable);
		}
	}

	private ExecutableChildNode getExecutable(Resource resource,
			ExecutableChildNode position) {
		ExecutableChildNode returns = null;
		if (position == null) {
			return null;
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
