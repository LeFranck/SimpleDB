package Test;

public class code
{
    public static int pow2(int power)
    {
        int ret = 1;
        for(int t=0;t<power;t++)
            ret <<= 1;
        return ret;
    }

    public static void main(String [] args)
    {
        final int total = 3;
        final int [] num = {0,0,0,1};
        boolean [] state = new boolean[total];

        for(int x=1;x<code.pow2(total);x++)
        {
            for(int i=0;i<total;i++)
            {
                // if the ith bit of x is not 0
                if( ( ( 1 << i ) & x ) != 0 )
                    state[i] = true;
                else
                    state[i] = false;
            }

            String output = new String();
            for(int i=0;i<total;i++)
            {
                if( state[i] == true )
                    output = output + num[i] + " ";
            }
            System.out.println(output);
        }
        System.exit(0);
    }
}