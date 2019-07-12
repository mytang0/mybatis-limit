package org.luming.mybatis.plugin.interceptor;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author luming
 */
@Data
@ConfigurationProperties(prefix = "mybatis.limit",
		ignoreInvalidFields = true)
public class LimitProperties {

	private static final int DEFAULT_LIMIT_SIZE = 1000;

	private static final List<String> DEFAULT_FILTER_METHODS = new ArrayList<>();

	private static final List<String> DEFAULT_PAGE_SIZE_FIELDS = new ArrayList<>();

	/**
	 * 是否启动自动limit
	 */
	private Boolean limitEnable = Boolean.FALSE;

	/**
	 * limit大小
	 */
	private Integer limitSize = DEFAULT_LIMIT_SIZE;

	/**
	 * true：mappedIds 为不需要检查名单
	 * false：mappedIds 为需要检查名单
	 */
	private Boolean reverse = Boolean.FALSE;

	/**
	 * 打印查询sql语句
	 */
	private Boolean printSql = Boolean.FALSE;

	/**
	 * pageSizes属性名
	 */
	private List<String> pageSizeFields = DEFAULT_PAGE_SIZE_FIELDS;

	/**
	 * 目标名单
	 */
	private List<String> mappedIds = Collections.emptyList();

	/**
	 * 过滤名单
	 */
	private List<String> filterMethods = DEFAULT_FILTER_METHODS;

	/**
	 * 为了便于匹配
	 */
	private Map<String, Object> limits = new ConcurrentHashMap<>();

	public void setMappedIds(List<String> mappedIds) {
		if (mappedIds != null) {
			this.mappedIds = mappedIds;
			mappedIds.stream()
					.filter(mappedId -> (mappedId != null && !mappedId.isEmpty()))
					.forEach(mappedId -> limits.put(mappedId, new Object()));
		}
	}

	public void setFilterMethods(List<String> filterMethods) {
		if (filterMethods != null) {
			this.filterMethods = filterMethods.stream()
					.filter(method -> (method != null && !method.isEmpty()))
					.map(String::toLowerCase).collect(Collectors.toList());
		}
	}

	static {
		Collections.addAll(DEFAULT_FILTER_METHODS, "page", "paging", "count");
		Collections.addAll(DEFAULT_PAGE_SIZE_FIELDS, "pageSize", "limit");
	}
}
