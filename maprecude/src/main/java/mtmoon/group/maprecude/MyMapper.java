package mtmoon.group.maprecude;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;
    private int count = 0;

    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
        //one.setBigint(0, 1L);
        
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
    	//矩阵A有m*n m = 
    	//矩阵B为n*l l = 
    	System.out.println("count: "+count);
    	//为矩阵A进行生成
    	long l = 40L;//1200L;//9999998L;
    	for (long i=0; i<l; i++) {
    		if (i == record.getBigint(0)) {
    			continue;
    		}
    		key.setBigint(0, record.getBigint(0));
    		
    		value.setString(0, "a");
    		value.setBigint(1, i);
    		long offset = 0;
    		if ("click".equals(record.getString(2))) {
    			offset = 1;
    		} else if ("cart".equals(record.getString(2))) {
    			offset = 2;
    		} else if ("collect".equals(record.getString(2))) {
    			offset = 3;
    		} else if ("alipay".equals(record.getString(2))) {
    			offset = 4;
    		}
    		
    		value.setBigint(2, 4*(record.getBigint(1)-1)+offset);
    		value.setBigint(3,1L);
    		
    		context.write(key, value);
    		
    	}
    	//为矩阵B进行生成 bjk = akj
    	long m = 40L;//99999L;
    	for (long i=0; i<m; i++) {
    		if (i == record.getBigint(0)) {
    			continue;
    		}
    		key.setBigint(0, i);
    		
    		
    		value.setString(0, "b");
    		value.setBigint(1, record.getBigint(0));
    		long offset = 0;
    		if ("click".equals(record.getString(2))) {
    			offset = 1;
    		} else if ("cart".equals(record.getString(2))) {
    			offset = 2;
    		} else if ("collect".equals(record.getString(2))) {
    			offset = 3;
    		} else if ("alipay".equals(record.getString(2))) {
    			offset = 4;
    		}
    		
    		value.setBigint(2, 4*(record.getBigint(1)-1)+offset);
    		value.setBigint(3,1L);
    		
    		context.write(key, value);
    		
    	}
    	//String w = record.getString(0);
    	/*System.out.println("recordNum:" + recordNum);
    	System.out.println("record:" + record.getBigint(0));
    	System.out.println("record:" + record.getBigint(1));
    	System.out.println("record:" + record.getString(2));*/
    	//System.out.println("count"+count);
    	count++;
    	
        //word.setString(0, w);
    	//context.write(onerec, one);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}