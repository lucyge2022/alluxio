#include <stdlib>
#include <string>
#include "htrie_map.h"

using namespace std;
using namespace tsl;
namespace alluxio {

// a single cache instance for housing a single dataset
class HTrieCache {
private:
    htrie_map<string, Payload> htrieMap;
public:
    HTrieCache(int burstThreshold);

    void Put(const std::string& path, const Payload& payload);
    bool Exists(const std::string& path) const;
    bool DeleteItem(const std::string& path);
    bool DeletePrefix(const std::string& prefixString);
}

} // namespace alluxio

