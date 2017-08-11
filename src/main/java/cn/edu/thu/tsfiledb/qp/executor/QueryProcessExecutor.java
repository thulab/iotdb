package cn.edu.thu.tsfiledb.qp.executor;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.auth2.dao.AuthDao;
import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.model.Role;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.kvmatch.KvMatchQueryRequest;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.iterator.MergeQuerySetIterator;
import cn.edu.thu.tsfiledb.qp.executor.iterator.PatternQueryDataSetIterator;
import cn.edu.thu.tsfiledb.qp.executor.iterator.QueryDataSetIterator;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.IndexQueryPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.MergeQuerySetPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.SeriesSelectPlan;
import cn.edu.thu.tsfiledb.qp.strategy.PhysicalGenerator;

import java.io.IOException;
import java.util.*;

public abstract class QueryProcessExecutor {

	protected ThreadLocal<Map<String, Object>> parameters = new ThreadLocal<>();
	protected ThreadLocal<Integer> fetchSize = new ThreadLocal<>();

	public QueryProcessExecutor() {
	}

	protected abstract TSDataType getNonReservedSeriesType(Path fullPath) throws PathErrorException;

	protected abstract boolean judgeNonReservedPathExists(Path fullPath);

	public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessorException {
		PhysicalGenerator transformer = new PhysicalGenerator(this);
		return transformer.transformToPhysicalPlan(operator);
	}

	public Iterator<QueryDataSet> processQuery(PhysicalPlan plan) throws QueryProcessorException {
		switch (plan.getOperatorType()) {
			case QUERY:
				SeriesSelectPlan query = (SeriesSelectPlan) plan;
				FilterExpression[] filterExpressions = query.getFilterExpressions();
				return new QueryDataSetIterator(query.getPaths(), getFetchSize(), this,
						filterExpressions[0], filterExpressions[1], filterExpressions[2]);
			case MERGEQUERY:
				MergeQuerySetPlan mergeQuery = (MergeQuerySetPlan) plan;
				List<SeriesSelectPlan> selectPlans = mergeQuery.getSeriesSelectPlans();
				if (selectPlans.size() == 1) {
					return processQuery(selectPlans.get(0));
				} else {
					return new MergeQuerySetIterator(selectPlans, getFetchSize(), this);
				}
			case INDEXQUERY:
				IndexQueryPlan indexQueryPlan = (IndexQueryPlan) plan;
				MManager mManager = MManager.getInstance();
				// check path and storage group
				Path path = indexQueryPlan.getPaths().get(0);
				if (!mManager.pathExist(path.getFullPath())) {
					throw new QueryProcessorException(String.format("The timeseries %s does not exist.", path));
				}
				try {
					mManager.getFileNameByPath(path.getFullPath());
				} catch (PathErrorException e) {
					e.printStackTrace();
					throw new QueryProcessorException(e.getMessage());
				}
				Path patterPath = indexQueryPlan.getPatterPath();
				if (!mManager.pathExist(patterPath.getFullPath())) {
					throw new QueryProcessorException(String.format("The timeseries %s does not exist.", patterPath));
				}
				try {
					mManager.getFileNameByPath(patterPath.getFullPath());
				} catch (PathErrorException e) {
					e.printStackTrace();
					throw new QueryProcessorException(e.getMessage());
				}
				// check index for metadata
				if (!mManager.checkPathIndex(path.getFullPath())) {
					throw new QueryProcessorException(String.format("The timeseries %s hasn't been indexed.", path));
				}
				KvMatchQueryRequest queryRequest = KvMatchQueryRequest
						.builder(indexQueryPlan.getPaths().get(0), indexQueryPlan.getPatterPath(),
								indexQueryPlan.getPatterStarTime(), indexQueryPlan.getPatterEndTime(),
								indexQueryPlan.getEpsilon())
						.startTime(indexQueryPlan.getStartTime()).endTime(indexQueryPlan.getEndTime()).build();
				if (indexQueryPlan.isHasParameter()) {
					queryRequest.setAlpha(indexQueryPlan.getAlpha());
					queryRequest.setBeta(indexQueryPlan.getBeta());
				}
				return new PatternQueryDataSetIterator(queryRequest, getFetchSize());
			default:
				throw new UnsupportedOperationException();
		}
	}

	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
		throw new UnsupportedOperationException();
	}

	public TSDataType getSeriesType(Path fullPath) throws PathErrorException {
		if (fullPath.equals(SQLConstant.RESERVED_TIME))
			return TSDataType.INT64;
		if (fullPath.equals(SQLConstant.RESERVED_FREQ))
			return TSDataType.FLOAT;
		return getNonReservedSeriesType(fullPath);
	}

	public boolean judgePathExists(Path pathStr) {
		if (SQLConstant.isReservedPath(pathStr))
			return true;
		else
			return judgeNonReservedPathExists(pathStr);
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize.set(fetchSize);
	}

	public int getFetchSize() {
		if (fetchSize.get() == null) {
			return 100;
		}
		return fetchSize.get();
	}

	public abstract QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter, int fetchSize, QueryDataSet lastData) throws ProcessorException;

	/**
	 * execute update command and return whether the operator is successful.
	 *
	 * @param path : update series path
	 * @param startTime start time in update command
	 * @param endTime end time in update command
	 * @param value - in type of string
	 * @return - whether the operator is successful.
	 */
	public abstract boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException;

	/**
	 * execute delete command and return whether the operator is successful.
	 *
	 * @param paths : delete series paths
	 * @param deleteTime end time in delete command
	 * @return - whether the operator is successful.
	 */
	public boolean delete(List<Path> paths, long deleteTime) throws ProcessorException {
		try {
			boolean result = true;
			MManager mManager = MManager.getInstance();
			Set<String> pathSet = new HashSet<>();
			for (Path p : paths) {
				if (!mManager.pathExist(p.getFullPath())) {
					throw new ProcessorException(String.format("Timeseries %s does not exist and cannot be delete its data", p.getFullPath()));
				}
				pathSet.addAll(mManager.getPaths(p.getFullPath()));
			}
			List<String> fullPath = new ArrayList<>();
			fullPath.addAll(pathSet);
			for (String path : fullPath) {
				result &= delete(new Path(path), deleteTime);
			}
			return result;
		} catch (PathErrorException e) {
			throw new ProcessorException(e.getMessage());
		}
	}

	/**
	 * execute delete command and return whether the operator is successful.
	 *
	 * @param path : delete series path
	 * @param deleteTime end time in delete command
	 * @return - whether the operator is successful.
	 */
	public abstract boolean delete(Path path, long deleteTime) throws ProcessorException;

	/**
	 * execute insert command and return whether the operator is successful.
	 *
	 * @param path path to be inserted
	 * @param insertTime - it's time point but not a range
	 * @param value value to be inserted
	 * @return - Operate Type.
	 */
	public abstract int insert(Path path, long insertTime, String value) throws ProcessorException;

	public abstract int multiInsert(String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues) throws ProcessorException;

	public MManager getMManager() {
		return MManager.getInstance();
	}

	public void addParameter(String key, Object value) {
		if(parameters.get() == null) {
			parameters.set(new HashMap<>());
		}
		parameters.get().put(key, value);
	}

	public Object getParameter(String key) {
		return parameters.get().get(key);
	}

	public void clearParameters() {
		if (parameters.get() != null) {
			parameters.get().clear();
		}
		if (fetchSize.get() != null) {
			fetchSize.remove();
		}
	}

	/**
	 *
	 * @param username updated user's name
	 * @param newPassword new password
	 * @return boolean
	 * @throws AuthException exception in update user
	 * @throws IOException 
	 * @throws NoSuchUserException 
	 */
	public boolean updateUser(String username,String newPassword) throws AuthException, IOException{
		return AuthDao.getInstance().modifyPassword(username, newPassword);
	}

	public boolean createUser(String username, String password) throws AuthException, IOException {
		return AuthDao.getInstance().addUser(username, password);
	}

	public boolean addPermissionToUser(String userName, String nodeName, int permissionId) throws AuthException {
		return false;
	}

	public boolean removePermissionFromUser(String userName, String nodeName, int permissionId) throws AuthException {
		return false;
	}

	public boolean deleteUser(String userName) throws AuthException, IOException {
		return AuthDao.getInstance().deleteUser(userName);
	}

	public boolean createRole(String roleName) throws AuthException, IOException {
		return AuthDao.getInstance().addRole(roleName);
	}

	public boolean addPermissionToRole(String roleName, long permission) throws AuthException, IOException {
		return AuthDao.getInstance().grantRolePermission(roleName, permission);
	}

	public boolean removePermissionFromRole(String roleName, long permission) throws AuthException, IOException {
		return AuthDao.getInstance().revokeRolePermission(roleName, permission);
	}

	public boolean deleteRole(String roleName) throws AuthException, IOException {
		return AuthDao.getInstance().deleteRole(roleName);
	}

	public boolean grantRoleToUser(String roleName, String username, String path) throws AuthException, PathErrorException, IOException {
		return AuthDao.getInstance().grantRoleOnPath(username, path, roleName);
	}

	public boolean revokeRoleFromUser(String roleName, String username, String path) throws AuthException, PathErrorException, IOException {
		return AuthDao.getInstance().revokeRoleOnPath(username, path, roleName);
	}

	public long getPermissionsOfUser(String username, String nodeName) throws AuthException, PathErrorException, IOException {
		return AuthDao.getInstance().getPermissionOnPath(username, nodeName);
	}

	public Object getRolesOfUser(String userName, String fullPath) throws AuthException {
		return AuthDao.getInstance().getRolesOnPath(userName, fullPath);
	}
	
	public Role[] getAllRoles() throws AuthException {
		return AuthDao.getInstance().getAllRoles();
	}
	
	public PhysicalPlan queryPhysicalOptimize(PhysicalPlan plan) {
		return plan;
	}

	public PhysicalPlan nonQueryPhysicalOptimize(PhysicalPlan plan) {
		return plan;
	}
	
	public abstract List<String> getAllPaths(String originPath) throws PathErrorException;

}
