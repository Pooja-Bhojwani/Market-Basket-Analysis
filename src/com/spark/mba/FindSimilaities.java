package com.spark.mba;


// STEP-0: import required classes and interfaces

import java.util.List;
import java.util.ArrayList;

//
import scala.Tuple2;
import scala.Tuple3;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
//
import org.dataalgorithms.util.Combination;

import com.spark.mba.Util;

/**
 * 
 * The FindAssociationRules class finds all association rules 
 * for a market basket data sets.
 *  
 * @author Mahmoud Parsian
 *
 */ 
public class FindSimilaities {
  
   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
//      if (args.length < 1) {
//         System.err.println("Usage: FindAssociationRules <transactions>");
//         System.exit(1);
//      }
      String transactionsFileName =  "./similarity_demo_input";

      // STEP-2: create a Spark context object
      SparkConf conf = new SparkConf()
    		    .setMaster("local")
    		    .setAppName("FindAssociationRules");
//      JavaSparkContext sc = new JavaSparkContext(conf);
      JavaSparkContext ctx = new JavaSparkContext(conf);
       
      // STEP-3: read all transactions from HDFS and create the first RDD 
      JavaRDD<String> transactions = ctx.textFile(transactionsFileName, 1);
      FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
  	  fs.delete(new Path("similarity"), true); // delete dir, true for recursive
  	 transactions.saveAsTextFile("similarity/1");
  	 

  	JavaPairRDD<String,List<String>> user_product_mapping =  transactions.mapToPair(
    		transaction -> {
    		 	if ((transaction == null) || (transaction.length() == 0) || transaction.startsWith("user_id")) {
    	            // no mapper output will be generated
    	            return null;
    	         }
    			String[] tokens = StringUtils.split(transaction,"|");
    			String user_id = tokens[0];
    			String[] products = StringUtils.split(tokens[1],",");
    			
    			List<String> products_list = new ArrayList<String>();
    			
    			if((user_id!= null) && (products!= null))
    			{
    				for (String item : products) {
    					products_list.add(item);
    	        }
    			}	
    			return new Tuple2<>(user_id,products_list);
    		}).filter(x->x!=null);
  	user_product_mapping.saveAsTextFile("similarity/2");
  	 
	JavaPairRDD<String, String> product_user_flatmap =  transactions.flatMapToPair(
		transaction -> {
			List< //we return a list of K,V pairs (Tuple2) from flatMapToPair 
			Tuple2<
				String, //movieID or userID
				String//rating, 1.0
			>
	    >  l = new ArrayList<>();
			
		 	if ((transaction == null) || (transaction.length() == 0) || transaction.startsWith("user_id")) {
	            // no mapper output will be generated
		 		return l.iterator();
	         }
			String[] tokens = StringUtils.split(transaction,"|");
			String user_id = tokens[0];
			String[] products = StringUtils.split(tokens[1],",");
//			String[] products_unique = new HashSet<String>(Arrays.asList(products)).toArray(new String[0]);

//			List<String> products_list = new ArrayList<String>();
			if((user_id!= null) && (products!= null))
			{
				for (String item : products) {
					l.add(new Tuple2<>(item, user_id));
//	        		products_list.add(item);
	        }
			}
//				return new Tuple2<>(user_id,products);
			return l.iterator();
		}).distinct();
	product_user_flatmap.saveAsTextFile("similarity/3");
	
//	Group by shingle
	JavaPairRDD<String, Iterable<String>> product_user_flatmap_grouped = product_user_flatmap.groupByKey();
	product_user_flatmap_grouped.saveAsTextFile("similarity/4");  
	
	JavaPairRDD<List<String>,Integer> user_combinations = 
			product_user_flatmap_grouped.flatMapToPair((Tuple2<String,Iterable<String>> transaction) -> {
				 List<String> list = new ArrayList<String>();
				 for (String item : transaction._2) {
					 list.add(item);
				 }
//						l.add(new Tuple2<>(item, user_id));
		        		
//	             List<String> list = Util.toList(transaction._2);
	             List<List<String>> combinations = Combination.findSortedCombinations(list, 2);
	             List<Tuple2<List<String>,Integer>> result = new ArrayList<Tuple2<List<String>,Integer>>();
	             for (List<String> combList : combinations) {
	                 if (combList.size() > 0) {
	                     result.add(new Tuple2<List<String>,Integer>(combList, 1));
	                 }
	             }
	             return result.iterator();
	      });
	user_combinations.saveAsTextFile("similarity/5");
	
	
	JavaPairRDD<List<String>, Integer> user_combinations_reduced = user_combinations.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
	user_combinations_reduced.saveAsTextFile("similarity/6");

	
////	Item-Item Similarity
	JavaPairRDD<List<String>,Integer> item_combinations = 
			user_product_mapping.flatMapToPair((Tuple2<String,List<String>> transaction) -> {
	             List<List<String>> combinations = Combination.findSortedCombinations(transaction._2, 2);
	             List<Tuple2<List<String>,Integer>> result = new ArrayList<Tuple2<List<String>,Integer>>();
	             for (List<String> combList : combinations) {
	                 if (combList.size() > 0) {
	                     result.add(new Tuple2<List<String>,Integer>(combList, 1));
	                 }
	             }
	             return result.iterator();
	      });
	item_combinations.saveAsTextFile("similarity/7");
	
	JavaPairRDD<List<String>, Integer> item_combinations_reduced = item_combinations.reduceByKey((Integer i1, Integer i2) -> i1 + i2); 
	item_combinations_reduced.saveAsTextFile("similarity/8");
	
	ctx.stop();
    ctx.close(); 
      
    System.exit(0);
   }
}

