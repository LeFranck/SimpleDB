package simpledb.opt;

import java.util.ArrayList;
import java.util.Collection;

import simpledb.parse.QueryData;
import simpledb.planner.QueryPlanner;
import simpledb.query.Plan;
import simpledb.query.ProjectPlan;
import simpledb.tx.Transaction;

public class SelingerPlanner implements QueryPlanner {
	   private Collection<TablePlanner> tableplanners = new ArrayList<TablePlanner>();

	   /**
	    * Creates an optimized left-deep query plan using the following
	    * heuristics.
	    * H1. Choose the smallest table (considering selection predicates)
	    * to be first in the join order.
	    * H2. Add the table to the join order which
	    * results in the smallest output.
	    */

	   public Plan createPlan(QueryData data, Transaction tx) {
	      
	      // Step 1:  Create a TablePlanner object for each mentioned table
	      for (String tblname : data.tables()) {
	         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx);
	         tableplanners.add(tp);
	      }
	      
	      DinamicTable dt = new DinamicTable(data,tableplanners);
	      dt.fill_first_floor();
	      dt.fill_second_floor();
	      
	      for(int i = dt.getStart_point(); i < dt.getR(); i++)
	      {
	    	  dt.set_plan(i);
	      }
	      
	      Plan pre_proyection = dt.get_final_plan();
	      //Plan plan_final = new ProjectPlan(pre_proyection, data.fields());
	      
	      return pre_proyection;

	      
//	      // Step 2:  Choose the lowest-size plan to begin the join order
//	      Plan currentplan = getLowestSelectPlan();
//	      
//	      // Step 3:  Repeatedly add a plan to the join order
//	      while (!tableplanners.isEmpty()) {
//	         Plan p = getLowestJoinPlan(currentplan);
//	         if (p != null)
//	            currentplan = p;
//	         else  // no applicable join
//	            currentplan = getLowestProductPlan(currentplan);
//	      }
//	      
//	      // Step 4.  Project on the field names and return
//	      return new ProjectPlan(currentplan, data.fields());
	   }
	   
	   private Plan getLowestSelectPlan() {
	      TablePlanner besttp = null;
	      Plan bestplan = null;
	      for (TablePlanner tp : tableplanners) {
		         Plan plan = tp.makeSelectPlan();
	         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
	            besttp = tp;
	            bestplan = plan;
	         }
	      }
	      tableplanners.remove(besttp);
	      return bestplan;
	   }
	   
	   private Plan getLowestJoinPlan(Plan current) {
	      TablePlanner besttp = null;
	      Plan bestplan = null;
	      for (TablePlanner tp : tableplanners) {
	         Plan plan = tp.makeJoinPlan(current);
	         if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
	            besttp = tp;
	            bestplan = plan;
	         }
	      }
	      if (bestplan != null)
	         tableplanners.remove(besttp);
	      return bestplan;
	   }
	   
	   private Plan getLowestProductPlan(Plan current) {
	      TablePlanner besttp = null;
	      Plan bestplan = null;
	      for (TablePlanner tp : tableplanners) {
	         Plan plan = tp.makeProductPlan(current);
	         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
	            besttp = tp;
	            bestplan = plan;
	         }
	      }
	      tableplanners.remove(besttp);
	      return bestplan;
	   }
}
