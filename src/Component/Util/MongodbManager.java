package Component.Util;

import com.letv.user_profile.UserProfile;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.bson.Document;
import org.xerial.snappy.Snappy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.set;


/**
 * Created by zhaoyibo on 2017/2/27.
 */
public class MongodbManager {
//    private static List<MongoClient> mongo = new ArrayList<MongoClient>();
    private String name;
    private List<MongoClientURI> address = new ArrayList<MongoClientURI>();
//    static Semaphore semaphore = new Semaphore(10);
    private String dbName;
    private String collection;
    private int collectionNum = 1;
    private int retryTimes = 3;
    private static TDeserializer deser = new TDeserializer();
    private static TSerializer ser = new TSerializer();
    private List<MongoClient> connections = null;
    public void init(String name, String address, String database, String collection) {
        setName(name);
        setAddress(address);
        setDbName(database);
        setCollection(collection);
    }
    public void init(String name, String address, String database, String collection, int collectionNum) {
        setName(name);
        setAddress(address);
        setDbName(database);
        setCollection(collection);
        setCollectionNum(collectionNum);
    }
    public boolean setUserExposure(String key, UserProfile userProfile) throws Exception {
        String jsonString = encodeProtoLine(userProfile);
        try {
//            semaphore.acquire();
            try {
                MongoCollection<Document> collection = getDBCollection(key);
                collection.updateOne(
                        eq("_id", key),
                        combine(set("long_time_exposures", jsonString), currentDate("lastModified")),
                        new UpdateOptions().upsert(true).bypassDocumentValidation(false));
            } finally {
//                semaphore.release();
            }
        } catch(InterruptedException ie) {
            ie.printStackTrace();
        }
        return true;
    }

    public boolean setUserExposureAndOfflineStatics(
            String key, UserProfile eUserProfile, UserProfile oUserProfile) throws Exception {
        String jsonString = encodeProtoLine(eUserProfile);
        String jsonString2 = encodeProtoLine(oUserProfile);
        try {
//            semaphore.acquire();
            try {
                MongoCollection<Document> collection = getDBCollection(key);
                collection.updateOne(
                        eq("_id", key),
                        combine(set("offline_statics", jsonString2),
                                set("long_time_exposures", jsonString),
                                currentDate("lastModified")),
                        new UpdateOptions().upsert(true).bypassDocumentValidation(false));
            } finally {
//                semaphore.release();
            }
        } catch(InterruptedException ie) {
            ie.printStackTrace();
        }
        return true;
    }

    public void initConnections() {
        connections = new ArrayList<MongoClient>();
        for (int i = 0; i < address.size(); i++) {
            try {
                MongoClient tmpMongo = new MongoClient(address.get(i));
                connections.add(tmpMongo);
                System.out.println("url:" + address.get(i));
            } catch (MongoException e) {
                System.out.println(e);
            }
        }
    }
    public MongoCollection<Document> getDBCollection(String key) throws Exception {
        long finger = hash64(key.getBytes(), key.length(), 19820125);
//        System.out.println(finger);
//        long global_collection_index = finger % (collectionNum * address.size());
        int divisor = collectionNum * address.size();
        BigInteger lll = BigInteger.valueOf(finger >>> 1)
                .multiply(BigInteger.valueOf(2))
                .add(BigInteger.valueOf((finger ^ ((finger >>> 1) << 1))));
        int global_collection_index = lll.mod(BigInteger.valueOf(divisor)).intValue();
        int connIndex = global_collection_index / collectionNum;
        int collectionIndex = global_collection_index % collectionNum;
        String alteredCollection = collection;
        if (collectionNum != 1) {
            alteredCollection = alteredCollection + "_" + collectionIndex;
        }
//        if (mongo.isEmpty()) {
//            System.out.println("init");
//            initConnections();
//        }
//        MongoDatabase database = mongo.get(connIndex.intValue()).getDatabase(dbName);
//        String colName = "user_profile_4";
//        MongoClient client = new MongoClient(address.get(connIndex));
        MongoClient client = getMongoClient(connIndex);
//        MongoDatabase database = client.getDatabase(colName);
        MongoDatabase database = client.getDatabase(dbName);
//        System.out.println(alteredCollection + " " + connIndex + " " + collectionIndex);
        return database.getCollection(alteredCollection);
    }

    private MongoClient getMongoClient(int connIndex) {
//        if (connections == null) {
//            initConnections();
//        }
        return connections.get(connIndex);
    }

    public static long hash64(final byte[] data, int length, int seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (seed&0xffffffffl)^(length*m);

        int length8 = length/8;

        for (int i=0; i<length8; i++) {
            final int i8 = i*8;
            long k =  ((long)data[i8+0]&0xff)      +(((long)data[i8+1]&0xff)<<8)
                    +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
                    +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
                    +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        switch (length%8) {
            case 7: h ^= (long)(data[(length&~7)+6]&0xff) << 48;
            case 6: h ^= (long)(data[(length&~7)+5]&0xff) << 40;
            case 5: h ^= (long)(data[(length&~7)+4]&0xff) << 32;
            case 4: h ^= (long)(data[(length&~7)+3]&0xff) << 24;
            case 3: h ^= (long)(data[(length&~7)+2]&0xff) << 16;
            case 2: h ^= (long)(data[(length&~7)+1]&0xff) << 8;
            case 1: h ^= (long)(data[length&~7]&0xff);
                h *= m;
        };

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }
    public long murMurHashInProject(byte[] data, int length) {
        int kFingerPrintSeed = 19820125;
        final long m = 0xc6a4a7935bd1e995l;
        final int r = 47;
        long h = (kFingerPrintSeed&0xffffffffl) ^ (length * m);
        int len_8 = length >> 3;

        int len_m;
        int left;
        for(len_m = 0; len_m < len_8; ++len_m) {
            left = len_m << 3;
            long var11 = ((data[left + 0] & 0xFFL) << 56) |
                    ((data[left + 1] & 0xFFL) << 48) |
                    ((data[left + 2] & 0xFFL) << 40) |
                    ((data[left + 3] & 0xFFL) << 32) |
                    ((data[left + 4] & 0xFFL) << 24) |
                    ((data[left + 5] & 0xFFL) << 16) |
                    ((data[left + 6] & 0xFFL) <<  8) |
                    ((data[left + 7] & 0xFFL) <<  0) ;
            var11 *= m;
            var11 ^= var11 >>> r;
            var11 *= m;

            h ^= var11;
            h *= m;
        }

        len_m = len_8 << 3;
        left = length - len_m;
        if(left != 0) {
            if(left >= 7) {
                h ^= data[length - 7] << 48;
            }
            if(left >= 6) {
                h ^= data[length - 6] << 40;
            }
            if(left >= 5) {
                h ^= data[length - 5] << 32;
            }
            if(left >= 4) {
                h ^= data[length - 4] << 24;
            }
            if(left >= 3) {
                h ^= data[length - 3] << 16;
            }
            if(left >= 2) {
                h ^= data[length - 2] << 8;
            }
            if(left >= 1) {
                h ^= data[length - 1];
            }
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
//        BigInteger lll = BigInteger.valueOf(h >>> 1).multiply(BigInteger.valueOf(2)).add(BigInteger.valueOf((h ^ ((h >>> 1) << 1))));
//        System.out.println("calculation h:" + (h >>> 1));
//        System.out.println("calculation biginteger:" + lll);
        return h;
    }

    private int unsingedLongMod(long dividend, int divisor) {
        long largeSide = dividend >>> 1;
        System.out.println("large:" + largeSide);
        int divisorSide = divisor >>> 1;
        System.out.println("divisor:" + divisorSide);
        int result = 0;
        if(divisor == 0) {
            return 0;
        }
        if (largeSide < divisorSide) {
            return (int) dividend;
        }
        int k, res=0;
        long c = 0;

        for (k = 0,c = divisor; largeSide >= (c >>> 1); c <<= 1, k++) {
            if (((dividend ^ c) >>> 1) < divisorSide) {
                res += 1<<k;
                System.out.println("k:" + k);
                result = (int) (dividend ^ ((long)res * divisor));
                break;
            }
        }
        return result;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setAddress(String address) {
        String strs[] = address.split(";");
        for (String str: strs) {
            MongoClientURI tmp = new MongoClientURI(str);
            this.address.add(tmp);
        }
    }

    public int getCollectionNum() {
        return collectionNum;
    }

    public void setCollectionNum(int collectionNum) {
        this.collectionNum = collectionNum;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    private String encodeProtoLine(UserProfile up) {
        Base64 encoder = new Base64();
        String result = "";
        try{
            byte tmpByteArray[] = ser.serialize(up);
            byte[] compressed = Snappy.compress(tmpByteArray);
            result = encoder.encodeToString(compressed);
        } catch (Exception e){
            System.out.println("exception caught: " + e);
        }
        return result;
    }
}
