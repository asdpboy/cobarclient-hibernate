package com.alibaba.cobar.client.support;

public class HQLBean {
	private String className ;
	
	private String classAlias ;
	
	private String tableAlias ;
	
	private String sql ;
	
	private String hql ;
	
	private String sqlType ;

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getClassAlias() {
		return classAlias;
	}

	public void setClassAlias(String classAlias) {
		this.classAlias = classAlias;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	public void setTableAlias(String tableAlias) {
		this.tableAlias = tableAlias;
	}

	public String getSqlType() {
		return sqlType;
	}

	public void setSqlType(String sqlType) {
		this.sqlType = sqlType;
	}
	
	public String getHql() {
		return hql;
	}

	public void setHql(String hql) {
		this.hql = hql;
	}

	@Override
	public String toString() {
		return "HQLBean [getClassName()=" + getClassName() + ", getClassAlias()="
                + getClassAlias() + ", getTableAlias()=" + getTableAlias() + ", getSql()=" + getSql() + "]";
	}
	
}
