package com.spark.mba;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

/**
 *
 * Some methods for FindAssociationRules class.
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    public static List<String> toList(String transaction) {
    	if ((transaction == null) || (transaction.length() == 0) || transaction.startsWith("order_id")) {
            // no mapper output will be generated
            return null;
         }      
         
         // TO-DO : We need to separate by | before we split by comma. 
         
         String[] tokens = StringUtils.split(transaction,"|");
//         
//        String[] items = StringUtils.split(StringUtils.split(line,"|")[1], ","); 
        String[] items = tokens[1].trim().split(",");
        List<String> list = new ArrayList<String>();
        for (String item : items) {
            list.add(item);
        }
        return list;
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || (list.isEmpty())) {
            return list;
        }
        //
        if ((i < 0) || (i > (list.size() - 1))) {
            return list;
        }
        //
        List<String> cloned = new ArrayList<String>(list);
        cloned.remove(i);
        //
        return cloned;
    }

}
