package org.luming.mybatis.plugin.interceptor;

import org.luming.mybatis.plugin.exceptions.InterceptorException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @author luming
 */
@Intercepts(
		{
				@Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
				@Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class})
		}
)
public class LimitInterceptor implements Interceptor {

	private final static Logger log = LoggerFactory.getLogger(LimitInterceptor.class);

	private final static String LIMIT_FLAG = "LIMIT";

	private final static String CHART_SPACE = " ";

	private final static String SQL_FIELD_NAME = "sql";

	private final static String ID_SEPARATOR = ".";

	private LimitProperties limitProperties = new LimitProperties();

	public LimitInterceptor(LimitProperties limitProperties) {
		if (limitProperties != null) {
			this.limitProperties = limitProperties;
		}
	}

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		if (!limitProperties.getLimitEnable()) {
			return invocation.proceed();
		}

		try {
			Object[] args = invocation.getArgs();
			MappedStatement ms = (MappedStatement) args[0];
			if (mappedSkip(ms.getId())) {
				checkPageSize(args[1]);
				return invocation.proceed();
			}
			if (listMatch(ms)) {
				return invocation.proceed();
			}
			RowBounds rowBounds = (RowBounds) args[2];
			//使用者很清楚自己在做什么
			if (rowBounds.getLimit() != RowBounds.NO_ROW_LIMIT) {
				checkPageSize(rowBounds.getLimit(), limitProperties.getLimitSize());
				return invocation.proceed();
			}
			BoundSql boundSql;
			if (args.length == 4) {
				boundSql = ms.getBoundSql(args[1]);
			} else {
				boundSql = (BoundSql) args[5];
			}
			String sql = boundSql.getSql();
			if (isNeedAppendLimit(sql)) {
				setValueByFieldName(boundSql, SQL_FIELD_NAME, appendLimit(sql));
				args[0] = newMappedStatement(ms, new BoundSqlSqlSource(boundSql));
			}
		} catch (InterceptorException ex) {
			throw ex;
		} catch (Exception ex) {
			//
		}

		return invocation.proceed();
	}

	@Override
	public Object plugin(Object o) {
		return Plugin.wrap(o, this);
	}

	@Override
	public void setProperties(Properties properties) {
	}

	private String appendLimit(String sql) {
		String appendSql = sql + CHART_SPACE + LIMIT_FLAG + CHART_SPACE + limitProperties.getLimitSize();
		printSql(appendSql);
		return appendSql;
	}

	private static boolean isNeedAppendLimit(String sql) {
		if (!containsIgnoreCase(sql, LIMIT_FLAG)) {
			return true;
		}
		try {
			Select select = (Select) CCJSqlParserUtil.parse(sql);
			SelectBody selectBody = select.getSelectBody();
			if (selectBody instanceof PlainSelect) {
				PlainSelect plainSelect = (PlainSelect) selectBody;
				if (Objects.nonNull(plainSelect.getLimit())) {
					return false;
				}
			} else if (selectBody instanceof SetOperationList) {
				SetOperationList setOperationList = (SetOperationList) selectBody;
				if (Objects.nonNull(setOperationList.getLimit())) {
					return false;
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		return true;
	}

	private void printSql(String sql) {
		if (limitProperties.getPrintSql()) {
			log.info("query sql: {}", sql);
		}
	}

	private boolean listMatch(MappedStatement ms) {
		String currentId = ms.getId();
		int lastIndexOf = currentId.lastIndexOf(ID_SEPARATOR);
		String parentId = (0 < lastIndexOf) ? currentId.substring(0, lastIndexOf) : null;
		Map<String, Object> limits = limitProperties.getLimits();
		if (limitProperties.getReverse()) {
			if (limits == null || limits.isEmpty()) {
				return false;
			}
			return limits.get(currentId) != null || limits.get(parentId) != null;
		}
		return limits == null || limits.isEmpty() || limits.get(currentId) == null || limits.get(parentId) == null;
	}

	private void checkPageSize(Object parameter) {
		List<String> pageSizeFields = limitProperties.getPageSizeFields();
		if (pageSizeFields != null && !pageSizeFields.isEmpty()) {
			pageSizeFields.forEach(pageSizeField -> checkPageSize(parameter, pageSizeField));
		}
	}

	private void checkPageSize(int realSize, int maxSize) {
		if (realSize > maxSize) {
			throw new InterceptorException(new StringBuilder(32)
					.append("pageSize too big: ")
					.append(realSize)
					.append(", maxSize: ")
					.append(maxSize)
					.toString());
		}
	}

	private void checkPageSize(Object parameter, String pageSizeField) {
		if (parameter == null) {
			return;
		}
		try {
			Object value;
			if (parameter instanceof Map) {
				if ((value = ((Map) parameter).get(pageSizeField)) != null && (value instanceof Number)) {
					checkPageSize(((Number) value).intValue(), limitProperties.getLimitSize());
				}
			} else {
				Field field;
				if ((field = getFieldByFieldName(parameter, pageSizeField)) != null) {
					if (field.isAccessible()) {
						value = field.get(parameter);
					} else {
						field.setAccessible(true);
						value = field.get(parameter);
						field.setAccessible(false);
					}
					if (value instanceof Number) {
						checkPageSize(((Number) value).intValue(), limitProperties.getLimitSize());
					}
				}
			}
		} catch (InterceptorException ex) {
			throw ex;
		} catch (Exception ex) {
			//
		}
	}

	/**
	 * filter非常大的时候可以考虑使用AC自动机提高匹配效率，否则没价值
	 */
	private boolean mappedSkip(String mappedId) {
		String realMethod = mappedId.substring(mappedId.lastIndexOf(ID_SEPARATOR)).toLowerCase();
		for (String methodLike : limitProperties.getFilterMethods()) {
			if (realMethod.contains(methodLike)) {
				return true;
			}
		}
		return false;
	}

	private static Field getFieldByFieldName(Object obj, String fieldName) {
		Class superClass = obj.getClass();

		while (superClass != Object.class) {
			try {
				return superClass.getDeclaredField(fieldName);
			} catch (NoSuchFieldException var4) {
				superClass = superClass.getSuperclass();
			}
		}
		return null;
	}

	private static void setValueByFieldName(Object obj, String fieldName, Object value) throws Exception {
		Field field = getFieldByFieldName(obj, fieldName);
		if (field == null) {
			return;
		}
		if (field.isAccessible()) {
			field.set(obj, value);
		} else {
			field.setAccessible(true);
			field.set(obj, value);
			field.setAccessible(false);
		}
	}

	private static boolean containsIgnoreCase(String str, String searchStr) {
		if (str == null || searchStr == null) {
			return false;
		}
		int len = searchStr.length();
		int max = str.length() - len;
		for (int i = 0; i <= max; i++) {
			if (str.regionMatches(true, i, searchStr, 0, len)) {
				return true;
			}
		}
		return false;
	}

	public static class BoundSqlSqlSource implements SqlSource {
		private BoundSql boundSql;

		BoundSqlSqlSource(BoundSql boundSql) {
			this.boundSql = boundSql;
		}

		@Override
		public BoundSql getBoundSql(Object parameterObject) {
			return boundSql;
		}
	}

	private MappedStatement newMappedStatement(MappedStatement ms, SqlSource newSqlSource) {
		MappedStatement.Builder builder = new MappedStatement.Builder(ms.getConfiguration(), ms.getId(), newSqlSource, ms.getSqlCommandType());

		builder.resource(ms.getResource());
		builder.fetchSize(ms.getFetchSize());
		builder.statementType(ms.getStatementType());
		builder.keyGenerator(ms.getKeyGenerator());
		if (ms.getKeyProperties() != null && ms.getKeyProperties().length > 0) {
			StringBuilder keyProperties = new StringBuilder();
			for (String keyProperty : ms.getKeyProperties()) {
				keyProperties.append(keyProperty).append(",");
			}
			keyProperties.delete(keyProperties.length() - 1, keyProperties.length());
			builder.keyProperty(keyProperties.toString());
		}
		builder.timeout(ms.getTimeout());
		builder.parameterMap(ms.getParameterMap());
		builder.resultMaps(ms.getResultMaps());
		builder.resultSetType(ms.getResultSetType());
		builder.cache(ms.getCache());
		builder.flushCacheRequired(ms.isFlushCacheRequired());
		builder.useCache(ms.isUseCache());

		return builder.build();
	}
}
