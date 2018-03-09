package storm.topology_two;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;

public class CounterMapUtils {

	public static Map<String, AtomicLong> stringCounter = Maps.newConcurrentMap();
	public static Map<String, AtomicLong> numberCounter = Maps.newConcurrentMap();
	public static Map<String, AtomicLong> signCounter = Maps.newConcurrentMap();
}
