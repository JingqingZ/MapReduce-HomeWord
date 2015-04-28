package mtmoon.group.maprecude;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
        System.out.println("reduce ini");
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        
    	HashMap< Long , HashMap<Long,Long> > A = new HashMap<Long,HashMap<Long,Long>>();
    	HashMap< Long , HashMap<Long,Long> > B = new HashMap<Long,HashMap<Long,Long>>();
    	//first key is k, value ,which is HashMap, is j and value 
    	
    	//Record is a/b k j value
    	
    	HashMap<Long,Long> suma = new HashMap<Long,Long>();
    	HashMap<Long,Long> sumb = new HashMap<Long,Long>();
    	
        while (values.hasNext()) {
        	Record val = values.next();
        	String type = val.getString(0);
        	Long k = val.getBigint(1); 
        	Long j = val.getBigint(2); 
        	Long kjval = val.getBigint(3); 
        	
        	//System.out.println(type + " " + k + " " + j + " " + kjval);
           
            if ("a".equals(type)) {
            	
            	if (A.containsKey(k)) {
            		//System.out.println("size before:" + A.get(k).size());
            		A.get(k).put(j, kjval);
            		//System.out.println("size after:" + A.get(k).size());
            		Long tmpsuma = suma.get(k) + kjval*kjval;
            		suma.put(k, tmpsuma);		
            	} else {
            		HashMap<Long,Long> tempMap = new HashMap<Long,Long>();
            		tempMap.put(j, kjval);
            		A.put(k, tempMap);
            		suma.put(k, 0L);
            		
            	}
                
                
            } else if("b".equals(type)) {
            	
            	
            	if (B.containsKey(k)) {
            		B.get(k).put(j, kjval);
            		Long tmpsumb = sumb.get(k) + kjval*kjval;
            		sumb.put(k, tmpsumb);		
            	} else {
            		HashMap<Long,Long> tempMap = new HashMap<Long,Long>();
            		tempMap.put(j, kjval);
            		B.put(k, tempMap);
            		sumb.put(k, 0L);
            		
            	}
            } else {
            	System.out.println("error");
            }

        }
        
        
        HashMap<Long,Double> Answer = new HashMap<Long,Double>();
        Iterator<Entry<Long , HashMap<Long,Long>>> iter = A.entrySet().iterator();
        while (iter.hasNext()) {//遍历A
        	
        	Entry<Long , HashMap<Long,Long>> entry = (Entry<Long , HashMap<Long,Long>>) iter.next();
        	Long k = entry.getKey();
        	HashMap<Long,Long> tempVecA = entry.getValue();
        	HashMap<Long,Long> tempVecB = B.get(k);
        	//遍历k的map
        	
        	Iterator<Entry<Long , Long>> iterk = tempVecA.entrySet().iterator();
        	long multi = 0;
        	while (iterk.hasNext()) {
        		Entry<Long , Long> entryk = (Entry<Long ,Long>) iterk.next();
        		Long j = entryk.getKey();
        		if (tempVecB != null && tempVecB.containsKey(j)) {
        			
        			multi += entryk.getValue()*tempVecB.get(j);	
        		}
        		
        	}
           	if (!suma.containsKey(k) || !sumb.containsKey(k)) {
        		continue;
        	}
        	double tempa = Math.sqrt(suma.get(k));
        	double tempb = Math.sqrt(sumb.get(k));
            double ans = multi / (tempa*tempb);
            if (ans == 0) {
            	continue;
            }
            Answer.put(k, ans);
        	
        	
        }
        
        
        List<Map.Entry<Long, Double>> infoIds = new ArrayList<Map.Entry<Long,Double>>(Answer.entrySet());
        
        Collections.sort(infoIds, new Comparator<Map.Entry<Long, Double>>() {   
            public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {      
            	if ((o2.getValue() - o1.getValue())>0)  
                    return 1;  
                  else if((o2.getValue() - o1.getValue())==0)  
                    return 0;  
                  else   
                    return -1;  
            }
        }); 
        //System.out.println("lalala");
        for (int i = 0; i < infoIds.size() && i<5; i++) {
        	System.out.println("lalala");
            result.setBigint(0, (Long)key.getBigint(0));
            result.setBigint(1, (Long)infoIds.get(i).getKey());
            result.setDouble(2, (Double)infoIds.get(i).getValue());
            context.write(result);
        }
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
