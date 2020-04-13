package simpledb.opt;

import java.util.ArrayList;
import java.util.Collection;

import simpledb.index.query.IndexJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.opt.TablePlanner;
import simpledb.parse.QueryData;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.record.Schema;

public class DinamicTable {

//listas tiene las permutaciones en orden de cantidad y creciente numericamente 
	private ArrayList<ArrayList<String>> permutaciones = new ArrayList<ArrayList<String>>();
	private ArrayList<TablePlanner> tableplanners = new ArrayList<TablePlanner>();
    private int[][] join_table;
    private int[] Can_join;
    private Plan[] Planes;
    private int[] Costos;
    private int n;
    private int r;
    private int start_point;
    private Predicate pred; 
    
    //n = numero de relaciones, r=numero de permutaciones utiles
    public DinamicTable(QueryData data, Collection<TablePlanner> col_tableplanners)
    {
    	n = data.tables().size();
    	r = 2^n -1;
    	join_table = new int[n][n];
    	Can_join = new int[r];
    	Planes = new Plan[r];
    	Costos = new int[r];
    	pred = data.pred();
	    for (TablePlanner tp : col_tableplanners) 
	    {
	    	tableplanners.add(tp);
	    }
    	fill_join_table(tableplanners);
    	fill_permutaciones();
    	fill_Can_join();
    	start_point = permutaciones.get(0).size() + permutaciones.get(1).size();
    }

    public void fill_first_floor()
    {
    	//y es el arreglo de permutaciones 
    	for(int i = 0; i < permutaciones.get(0).size();i++)
    	{
    		String bin = permutaciones.get(0).get(i);
    		for(int j = 0; j < bin.length(); j++)
    		{
    			if(bin.charAt(j)=='1')
    			{
    		         Plan plan = tableplanners.get(j).makeSelectPlan();
    		         Planes[i] = plan;
    			}
    		}
    	}
    } 
    
    public void fill_second_floor()
    {
    	for(int i = 0; i < permutaciones.get(1).size(); i++)
    	{
	    	String bin = get_relacion(i+n);
	    	ArrayList<Integer> involucrados = get_relaciones_involucradas(bin);
	    	for(int j = 0; j < involucrados.size(); j++)
	    	{
	    		int izq = involucrados.get(0);
	    		int der = involucrados.get(1);
	        	String pre = new String(new char[n]).replace("\0", "0");
	        	String izq_bin = pre.substring(0,izq)+'1'+pre.substring(izq+1);
	        	String der_bin = pre.substring(0,der)+'1'+pre.substring(der+1);	    		
	        	Plan izq_plan = get_plan(izq_bin);
	        	TablePlanner der_plan = tableplanners.get(der);
	        	//if Can join	        	
	        	Plan join = der_plan.makeJoinPlan(izq_plan);	        	
	        	
	        	
	        	//Plan join = (make_join_plan(der_plan,izq_plan));
	        	Planes[i+n] = join;
	        	//else 
	        	//product plan
	    	}
    	}
    	
    } 
    
    private void fill_join_table(Collection<TablePlanner> tableplanners)
    {
    	int i=0 , j=0;
    	for (TablePlanner tp1 : tableplanners)
    	{
    		for (TablePlanner tp2 : tableplanners)
    		{
    			if(!tp1.equals(tp2))
    			{
    				Schema s1 = tp1.get__Myschema();
    				Schema s2 = tp2.get__Myschema();
    				Predicate p = tp1.getMypred();
    				Predicate p_join = p.joinPred(s1, s2);
    				if(!(p_join==null))
    				{
    					join_table[i][j] = 1;
    					join_table[j][i] = 1;
    				}
    			}
    			j++;
    		}
    		j = 0;
    		i++;
    	}
    }
    
    private void fill_Can_join()
    {
    	Can_join = new int[r];
    	int cont = 0;
    	for(int i = 0; i < permutaciones.size();i++)
    	{
    		for(int j = 0; j < permutaciones.get(i).size(); j++)
    		{
    			if(i>0){
	    			String comb = permutaciones.get(i).get(j);
	    			for(int k = 0; k < comb.length();k++)
	    			{
	    				if(comb.charAt(k) == '1')
	    				{
	    	    			for(int l = 0; l < comb.length();l++)
	    	    			{
	    	    				if(comb.charAt(l) == '1' && k!=l)
	    	    				{
	    	    					if(join_table[k][l]==1)
	    	    					{
	    	    						Can_join[cont] = 1;
	    	    					}
	    	    				}
	    	    				
	    	    			}
	    				}
	    			}
    			}else{
    				Can_join[cont] = 0;
    			}
				cont++;
    		}
    	}
    }
    
    private void fill_permutaciones()
    {
		for(int j = 0;j<n;j++)
		{
			permutaciones.add(new ArrayList<String>());
		}

		for(int i = 1; i < r + 1 ; i++)
		{
		    String num = (Integer.toBinaryString(i));
		    String bin = num;
		    if(num.length()<n)
		    {
		    	String zero = "0";
		    	String pre = new String(new char[n-num.length()]).replace("\0", zero);
		    	bin = pre.concat(num);
		    }
		    int count = 0;

			for(int j = 0; j < n; j++)
			{
				if(bin.charAt(j)=='1'){count++;}
			}
			permutaciones.get(count-1).add(bin);
			count = 0;
		}
    }

    private String get_relacion(int i)
    {
    	int cont = 0;
    	for(int j = 0; j < permutaciones.size(); j++)
    	{
    		for(int k = 0; k < permutaciones.get(j).size();k++)
    		{
    			if(cont==i)
    			{
    				return permutaciones.get(j).get(k);
    			}
    			cont++;
    		}
    	}
    	return "INDICE SUPERIOR A LA CANTIDAD DE COMBINACIONES";
    }
    
    private Plan get_plan(String bin)
    {
    	int cont = 0;
    	for(int j = 0; j < permutaciones.size(); j++)
    	{
    		for(int k = 0; k < permutaciones.get(j).size();k++)
    		{
    			if(permutaciones.get(j).get(k).equals(bin))
    			{
    				return Planes[cont];
    			}
    			cont++;
    		}
    	}
    	return null;
    }


    
    private Plan make_join_plan(Plan p1, Plan p2)
    {
    	Schema p1_sch = p1.schema();
    	Schema p2_sch = p2.schema();
    	Predicate joinpred = pred.joinPred(p1_sch, p2_sch);
    	if (joinpred == null)
    		return null;
    	Plan p = null;
    	//Plan p = makeIndexJoin(p2, p1_sch);
    	//if (p == null)
    		//p = makeProductJoin(p1, p1_sch);
    	return p;
    }
    
    private String involucrados_to_permutacion(ArrayList<Integer> involucrados)
    {
    	String pre = new String(new char[n]).replace("\0", "0");
    	for(Integer i : involucrados)
    	{
    		pre= pre.substring(0,i)+'1'+pre.substring(i+1);
    	}
    	return pre;
    }
    
    private ArrayList<Integer> get_relaciones_involucradas(String bin)
    {
    	ArrayList<Integer> output = new ArrayList<Integer>();
    	for(int i = 0; i < bin.length(); i++)
    	{
    		if(bin.charAt(i)=='1')
    		{
    			output.add(i);
    		}
    	}
    	return output;
    }

    private Plan get_cheaper_plan(ArrayList<Plan> Candidatos)
    {
	    Plan bestplan = null;
	    for (Plan jp : Candidatos) 
	    {
	    	if (jp != null && (bestplan == null || jp.recordsOutput() < bestplan.recordsOutput()))
	    	{
	    			bestplan = jp;
	    	}
    	}
	    return bestplan;
    }
    
    public void set_plan(int i)
    {
    	String bin = get_relacion(i);
    	int grupo = bin.replaceAll("0","").length();
    	ArrayList<Integer> involucrados = get_relaciones_involucradas(bin);
    	ArrayList<Plan> Candidatos = new ArrayList<Plan>();
    	//buscara la solución en los grupos menores
    	for(int j = 0; j < involucrados.size(); j++)
    	{
        	ArrayList<Integer> izq = new ArrayList<Integer>();
        	for(Integer aux : involucrados)
        		izq.add(aux);
        	int der = izq.remove(j);
        	String izq_bin = involucrados_to_permutacion(izq);
        	//String der_bin = involucrados_to_permutacion(der);
        	Plan izq_plan = get_plan(izq_bin);
        	//Cambiar der_plan a busqueda de plan en la tabla
        	TablePlanner der_plan = tableplanners.get(der);
        	//if Can join
        	Candidatos.add(der_plan.makeJoinPlan(izq_plan));
        	//else 
        	//product plan
    	}
    	//Elegir el plan minimo desde candidatos
    	Plan minimo = get_cheaper_plan(Candidatos);
    	Planes[i] = minimo;
    }
    
    public Plan get_final_plan()
    {
    	return Planes[r-1];
    }
    
    public int getN() {
		return n;
	}

	public int getR() {
		return r;
	}

	
	public int getStart_point() {
		return start_point;
	}

    
    
}
