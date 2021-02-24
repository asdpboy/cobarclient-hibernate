package com.alibaba.cobar.client.router.rules.support;

import org.apache.commons.lang.Validate;
/**
 * @author yanhongqi
 * @since 1.0
 */
public class ModFunction implements IFunction2<Object, Long> {
    private Long modDenominator;
    
    public ModFunction(Long modDenominator)
    {
        Validate.notNull(modDenominator);
        this.modDenominator = modDenominator;
    }
    
    public Long apply(Object input) {
        Validate.notNull(input);
        //特殊处理时针对由字符串和数字组合而成的分库key，允许最大分库个数为9个
        String i = String.valueOf(input).substring(String.valueOf(input).length() - 1) ;
        Long result = Long.parseLong(i) % this.modDenominator;
        return result;
    }

    public void setModDenominator(Long modDenominator) {
        Validate.notNull(modDenominator);
        this.modDenominator = modDenominator;
    }

    public Long getModDenominator() {
        return modDenominator;
    }

}
