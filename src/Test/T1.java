package Test;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;


public class T1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Collection<String> aux = new ArrayList<String>();
		aux.add("RON");
		aux.add("CON");
		aux.add("PON");
		aux.add("GON");
		ArrayList<String> aux2 = new ArrayList<String>();
	    for (String tp : aux) 
	    {
	    	aux2.add(tp);
	    }
	    
	    ArrayList<Integer> involucrados = new ArrayList<Integer>();
	    involucrados.add(0);
	    involucrados.add(3);
	   	String pre = new String(new char[4]).replace("\0", "0");
    	for(Integer i : involucrados)
    	{
    		pre= pre.substring(0,i)+'1'+pre.substring(i+1);
    	}
    	System.out.println(pre);


	    String[] y = aux.toArray(new String[0]);
		System.out.println(aux.toArray(new String[0])[0]);
		

		//Bruteza
		int k = 4;
		ArrayList<ArrayList<String>> listas = new ArrayList<ArrayList<String>>();
		for(int j = 0;j<k;j++)
		{
			listas.add(new ArrayList<String>());
		}

		for(int i = 1; i < Math.pow(k, 2); i++)
		{
		    String num = (Integer.toBinaryString(i));
		    String bin = num;
		    if(num.length()<k)
		    {
		    	String zero = "0";
		    	String pre2 = new String(new char[k-num.length()]).replace("\0", zero);
		    	bin = pre2.concat(num);
		    }
		    int count = 0;

			for(int j = 0; j < k; j++)
			{
				if(bin.charAt(j)=='1'){count++;}
			}
			listas.get(count-1).add(bin);
			count = 0;
		}

		for(int i = 0; i < listas.size(); i++)
		{
			for(int j = 0; j < listas.get(i).size(); j++)
			{
				System.out.println(listas.get(i).get(j));
			}
		}
		
	}

}