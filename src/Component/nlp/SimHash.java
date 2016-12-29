package Component.nlp;
import java.math.BigInteger;
import java.util.Map;

public class SimHash
{


    static BigInteger simHash(Map<String,Double> tf,int hashbits){
        int[] v = new int[hashbits];
        for(String word:tf.keySet()){
            BigInteger t = hash(word,hashbits);
            for (int i = 0; i < hashbits; i++)
            {
                BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                if (t.and(bitmask).signum() != 0)
                {
                    v[i] += 1*tf.get(word);
                }
                else
                {
                    v[i] -= 1*tf.get(word);;
                }
            }
        }
        BigInteger fingerprint = new BigInteger("0");
        for (int i = 0; i <hashbits; i++)
        {
            if (v[i] >= 0)
            {
                fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
            }
        }
        return fingerprint;
    }

    static private BigInteger hash(String source,int hashbits)
    {
        if (source == null || source.length() == 0)
        {
            return new BigInteger("0");
        }
        else
        {
            char[] sourceArray = source.toCharArray();
            BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(hashbits).subtract(
                    new BigInteger("1"));
            for (char item : sourceArray)
            {
                BigInteger temp = BigInteger.valueOf((long) item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if (x.equals(new BigInteger("-1")))
            {
                x = new BigInteger("-2");
            }
            return x;
        }
    }
    static public int hammingDistance(BigInteger s2,BigInteger s1,int hashbits)
    {
        BigInteger m = new BigInteger("1").shiftLeft(hashbits).subtract(
                new BigInteger("1"));
        BigInteger x = s1.xor(s2).and(m);
        int tot = 0;
        while (x.signum() != 0)
        {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }
    public static void main(String[] args)
    {

    }
}