package mtmoon.group.maprecude;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Combiner模板。请用真实逻辑替换模板内容
 */
public class MyCombiner implements Reducer {
    private Record count;

    public void setup(TaskContext context) throws IOException {
        count = context.createMapOutputValueRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        HashMap<String, Long> tempSave = new HashMap<String , Long>();

        while (values.hasNext()) {
            Record val = values.next();
            String pair = val.getString(0) + "#" + val.getBigint(1) + "#" + val.getBigint(2); 
            
            long v = 0L;
            if (tempSave.containsKey(pair)) {
            	v = tempSave.get(pair) + 1;            	
            } else {
            	v = 1;
            }
            tempSave.put(pair, v);
            
        }
        
        Iterator<Map.Entry<String, Long>> iter = tempSave.entrySet().iterator();

        while (iter.hasNext()) {
        	Map.Entry< String , Long> entry = (Map.Entry< String , Long>) iter.next();

        	String p = (String) entry.getKey();
        	Long v = entry.getValue();
        	
        	String[] array = p.split("#");
        	
        	count.setString(0, array[0]);
        	count.setBigint(1, new Long(array[1]));
        	count.setBigint(2, new Long(array[2]));
        	count.setBigint(3, v);
        	context.write(key, count);
        }
        
        
        
    	/*long c = 0;
        while (values.hasNext()) {
            Record val = values.next();
            c += val.getBigint(0);
        }
        count.set(0, c);
        context.write(key, count);*/
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
