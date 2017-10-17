package PublicCache;

/**
 * Created by c00491729 on 2017/10/17.
 */
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheTime {
    private static CacheTime cacheTime;
    private Map<Object, Object> cacheTimes;
    private CacheTime() {
        cacheTimes =new ConcurrentHashMap<Object, Object>();
    }
    /**
     * 获取唯一实例
     * @return instance
     */
    public static CacheTime getInstance() {
        if (cacheTime ==null) {
            synchronized (CacheTime.class) {
                if (cacheTime ==null) {
                    cacheTime =new CacheTime();
                }
            }
        }
        return cacheTime;
    }

    /**
     * 获取所有cache信息
     * @return cacheItems
     */
    public Map<Object, Object> getCacheItems() {
        return this.cacheTimes;
    }

    /**
     * 清空cache
     */
    public void clearAllItems() {
        cacheTimes.clear();
    }

    /**
     * 获取指定cache信息
     * @return cacheItem
     */
    public Object getCacheItem(Object key) {
        if (cacheTimes.containsKey(key)) {
            return cacheTimes.get(key);
        }
        return null;
    }

    /**
     * 存放cache信息
     */
    public void putCacheItem(Object key,Object value) {
        if (!cacheTimes.containsKey(key)) {
            cacheTimes.put(key, value);
        }
    }

    /**
     * 删除一个cache
     */
    public void removeCacheItem(Object key) {
        if (cacheTimes.containsKey(key)) {
            cacheTimes.remove(key);
        }
    }

    /**
     * 获取cache长度
     * @return size
     */
    public int getSize() {
        return cacheTimes.size();
    }

}
