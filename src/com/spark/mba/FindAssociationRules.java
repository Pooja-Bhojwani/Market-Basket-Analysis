package com.spark.mba;


// STEP-0: import required classes and interfaces

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
//
import scala.Tuple2;
import scala.Tuple3;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
//
import org.dataalgorithms.util.Combination;

/**
 * 
 * The FindAssociationRules class finds all association rules 
 * for a market basket data sets.
 *  
 * @author Mahmoud Parsian
 *
 */ 
public class FindAssociationRules {
  
   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
//      if (args.length < 1) {
//         System.err.println("Usage: FindAssociationRules <transactions>");
//         System.exit(1);
//      }
      String transactionsFileName =  "./spark_association_demo_input";

      // STEP-2: create a Spark context object
      SparkConf conf = new SparkConf()
    		    .setMaster("local")
    		    .setAppName("FindAssociationRules");
//      JavaSparkContext sc = new JavaSparkContext(conf);
      JavaSparkContext ctx = new JavaSparkContext(conf);
       
      // STEP-3: read all transactions from HDFS and create the first RDD 
      JavaRDD<String> transactions = ctx.textFile(transactionsFileName, 1);
      FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
  	  fs.delete(new Path("rules"), true); // delete dir, true for recursive
  	 transactions.saveAsTextFile("rules/1");
//      transactions.saveAsTextFile("/rules/output/1");

      // STEP-4: generate frequent patterns
      // PairFlatMapFunction<T, K, V>     
      // T => Iterable<Tuple2<K, V>>
      JavaPairRDD<List<String>,Integer> patterns = 
         transactions.flatMapToPair(new PairFlatMapFunction<
                                                             String,        // T
                                                             List<String>,  // K
                                                             Integer        // V
                                                           >() {
         @Override
         public Iterator<Tuple2<List<String>,Integer>> call(String transaction) {
            List<String> list = Util.toList(transaction);
            List<Tuple2<List<String>,Integer>> result = new ArrayList<Tuple2<List<String>,Integer>>();
            if(list!= null)
            {
            List<List<String>> combinations = Combination.findSortedCombinations(list);
            for (List<String> combList : combinations) {
                 if (combList.size() > 0) {
                   result.add(new Tuple2<List<String>,Integer>(combList, 1));
                 }
            }
            }
            return result.iterator();
         }
      });    
//      patterns.saveAsTextFile("./rules/output/2");
      patterns.saveAsTextFile("rules/2");
//      System.out.println("File 2 saved");
//       STEP-5: combine/reduce frequent patterns
      JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(new Function2<Integer, Integer, Integer>() {
         @Override
         public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
         }
      });    
      combined.saveAsTextFile("rules/3");
//      transactions.saveAsTextFile("rules/1");
//      fs.delete(new Path("rules_3"), true);
//      combined.saveAsTextFile("rules_3/3");
    
      // now, we have: patterns(K,V)
      //      K = pattern as List<String>
      //      V = frequency of pattern
      // now given (K,V) as (List<a,b,c>, 2) we will 
      // generate the following (K2,V2) pairs:
      //
      //   (List<a,b,c>, T2(null, 2))
      //   (List<a,b>,   T2(List<a,b,c>, 2))
      //   (List<a,c>,   T2(List<a,b,c>, 2))
      //   (List<b,c>,   T2(List<a,b,c>, 2))


      // STEP-6: generate all sub-patterns
      // PairFlatMapFunction<T, K, V>     
      // T => Iterable<Tuple2<K, V>>
      JavaPairRDD<List<String>,Tuple2<List<String>,Integer>> subpatterns = 
         combined.flatMapToPair(new PairFlatMapFunction<
          Tuple2<List<String>, Integer>,   // T
          List<String>,                    // K
          Tuple2<List<String>,Integer>     // V
        >() {
       @Override
       public Iterator<Tuple2<List<String>,Tuple2<List<String>,Integer>>> 
          call(Tuple2<List<String>, Integer> pattern) {
            List<Tuple2<List<String>,Tuple2<List<String>,Integer>>> result = 
               new ArrayList<Tuple2<List<String>,Tuple2<List<String>,Integer>>>();
            List<String> list = pattern._1;
            Integer frequency = pattern._2;
            result.add(new Tuple2(list, new Tuple2(null,frequency)));
            if (list.size() == 1) {
               return result.iterator();
            }
            
            // pattern has more than one items
            // result.add(new Tuple2(list, new Tuple2(null,size)));
            for (int i=0; i < list.size(); i++) {
               List<String> sublist = Util.removeOneItem(list, i);
               result.add(new Tuple2(sublist, new Tuple2(list, frequency)));
            }
            return result.iterator();
        }
      });
////      subpatterns.saveAsTextFile("/rules/output/4");
      subpatterns.saveAsTextFile("rules/4");
//        
      // STEP-6: combine sub-patterns
      JavaPairRDD<List<String>,Iterable<Tuple2<List<String>,Integer>>> rules = subpatterns.groupByKey();       
//      rules.saveAsTextFile("/rules/output/5");
     rules.saveAsTextFile("rules/5");
     System.out.println("Check rules");
//      System.out.println(rules);
      

      // STEP-7: generate association rules      
      // Now, use (K=List<String>, V=Iterable<Tuple2<List<String>,Integer>>) 
      // to generate association rules
      // JavaRDD<R> map(Function<T,R> f)
      // Return a new RDD by applying a function to all elements of this RDD.
      JavaRDD<List<Tuple3<List<String>,List<String>,Double>>> assocRules = rules.map(new Function<
          Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>>,     // T: input 
          List<Tuple3<List<String>,List<String>,Double>>                   // R: ( ac => b, 1/3): T3(List(a,c), List(b),  0.33)
                                                                           //    ( ad => c, 1/3): T3(List(a,d), List(c),  0.33)
         >() {
        @Override
        public List<Tuple3<List<String>,List<String>,Double>> call(Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>> in) {
            List<Tuple3<List<String>,List<String>,Double>> result = 
               new ArrayList<Tuple3<List<String>,List<String>,Double>>();
            List<String> fromList = in._1;
            Iterable<Tuple2<List<String>,Integer>> to = in._2;
            List<Tuple2<List<String>,Integer>> toList = new ArrayList<Tuple2<List<String>,Integer>>();
            Tuple2<List<String>,Integer> fromCount = null;
            for (Tuple2<List<String>,Integer> t2 : to) {
               // find the "count" object
               if (t2._1 == null) {
                    fromCount = t2;
               }
               else {
                  toList.add(t2);
               }
            }
            
            // Now, we have the required objects for generating association rules:
            //  "fromList", "fromCount", and "toList"
            if (toList.isEmpty()) {
               // no output generated, but since Spark does not like null objects, we will fake a null object
               return result; // an empty list
            } 
            
            // now using 3 objects: "from", "fromCount", and "toList",
            // create association rules:
            for (Tuple2<List<String>,Integer>  t2 : toList) {
               double confidence = (double) t2._2 / (double) fromCount._2;
               List<String> t2List = new ArrayList<String>(t2._1);
               t2List.removeAll(fromList);
               result.add(new Tuple3(fromList, t2List, confidence));
            }
          return result;
        }
      });   
      assocRules.saveAsTextFile("rules/6");
//      // done
      ctx.stop();
      ctx.close(); 
      
      System.exit(0);
   }
}

