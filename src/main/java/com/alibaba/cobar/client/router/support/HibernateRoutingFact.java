package com.alibaba.cobar.client.router.support;

/**
 * @author yanhongqi
 * @since 1.0
 */
public class HibernateRoutingFact {
	/**
	 * Entity Class of save|get|update|delete and so on
	 */
	private String method ;
	
	/**
	 * the argument of method
	 */
	private Object argument ;
	
    public HibernateRoutingFact(){}
	
    public HibernateRoutingFact(String method , Object argument){
    	this.method = method ;
    	this.argument = argument ;
    }
    
	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public Object getArgument() {
		return argument;
	}

	public void setArgument(Object argument) {
		this.argument = argument;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((method == null) ? 0 : method.hashCode());
		result = prime * result
				+ ((argument == null) ? 0 : argument.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HibernateRoutingFact other = (HibernateRoutingFact) obj;
		if (method == null) {
			if (other.method != null)
				return false;
		} else if (!method.equals(other.method))
			return false;
		if (argument == null) {
			if (other.argument != null)
				return false;
		} else if (!argument.equals(other.argument))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "HibernateRoutingFact [method=" + method + ", argument=" + argument
				+ "]";
	}
}
