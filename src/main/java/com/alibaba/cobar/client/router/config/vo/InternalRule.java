/**
 * Copyright 1999-2011 Alibaba Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.alibaba.cobar.client.router.config.vo;

public class InternalRule {

    private String className;
    private String methodName ;
    private String shardingExpression;
    private String shards;
    
    /**
     * sharding expression for table 
     */
    private String tshardingExpression ;
    /**
     * shard of table name
     * the value may be the table of name , maby be the expression like ${tab}0, ${tab} will replace by tableName.
     */
    private String tshards ;
    
    /**
     * this field is not used for now, because it's still in leverage whether
     * it's proper to bind merging information into a routing concern.
     */
    private String merger;
    
    public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public String getShardingExpression() {
        return shardingExpression;
    }

    public void setShardingExpression(String shardingExpression) {
        this.shardingExpression = shardingExpression;
    }

    public String getShards() {
        return shards;
    }

    public void setShards(String shards) {
        this.shards = shards;
    }

    /**
     * set the bean name of merger to use.
     * 
     * @param merger, the bean name in the container.
     */
    public void setMerger(String merger) {
        this.merger = merger;
    }

    /**
     * @return the bean name of the merger.
     */
    public String getMerger() {
        return merger;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((className == null) ? 0 : className.hashCode());
        result = prime * result
                + ((shardingExpression == null) ? 0 : shardingExpression.hashCode());
        result = prime * result + ((shards == null) ? 0 : shards.hashCode());
        result = prime * result + ((methodName == null) ? 0 : methodName.hashCode());
        result = prime * result
                + ((tshardingExpression == null) ? 0 : tshardingExpression.hashCode());
        result = prime * result + ((tshards == null) ? 0 : tshards.hashCode());
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
        InternalRule other = (InternalRule) obj;
        if (className == null) {
            if (other.className != null)
                return false;
        } else if (!className.equals(other.className))
            return false;
        if (shardingExpression == null) {
            if (other.shardingExpression != null)
                return false;
        } else if (!shardingExpression.equals(other.shardingExpression))
            return false;
        if (shards == null) {
            if (other.shards != null)
                return false;
        } else if (!shards.equals(other.shards))
            return false;
        if (methodName == null) {
            if (other.methodName != null)
                return false;
        } else if (!methodName.equals(other.methodName))
            return false;
        if (tshardingExpression == null) {
            if (other.tshardingExpression != null)
                return false;
        } else if (!tshardingExpression.equals(other.tshardingExpression))
            return false;
        if (tshards == null) {
            if (other.tshards != null)
                return false;
        } else if (!tshards.equals(other.tshards))
            return false;
        return true;
    }

    public String getTshardingExpression() {
		return tshardingExpression;
	}

	public void setTshardingExpression(String tshardingExpression) {
		this.tshardingExpression = tshardingExpression;
	}

	public String getTshards() {
		return tshards;
	}

	public void setTshards(String tshards) {
		this.tshards = tshards;
	}

	@Override
    public String toString() {
        return "InternalRule [className=" + className + ", shardingExpression="
                + shardingExpression + ", shards=" + shards + ", methodName=" + methodName + ", tshardingExpression="
                        + tshardingExpression + ", tshards=" + tshards+ "]";
    }
}
