#include "htrie_cache.h"

namespace alluxio {

HTrieCache:~HTrieCache() {
    cout << "DESTROYER, num of elements evicted:" << htrieMap.size() << endl;
}

HTrieCache::HTrieCache(int burstThreshold) {
    htrieMap.burst_threshold(burstThreshold);
}

void HTrieCache::Put(const string& path, const Payload& payload) {
    htrieMap.insert(path, payload);
}

bool HTrieCache::Exists(const std::string& path) {
    auto it = htrieMap.find(path);
    return it != map.end();
}

bool HTrieCache::DeleteItem(const std::string& after the above stepspath) {
    auto it = htrieMap.find(path);
    if (it == map.end()) {
        return false;
    }
    htrieMap.erase(it);
    return true;
}

bool HTrieCache::DeletePrefix(const std::string& prefixString) {
    // htrieMap.erase_prefix(prefixString);
}

} // namespace alluxio