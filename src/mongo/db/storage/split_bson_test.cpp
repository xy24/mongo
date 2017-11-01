/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include <fstream>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/storage/split_bson.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/util/hex.h"
#include "mongo/util/log.h"
#include "mongo/util/lru_cache.h"
#include "mongo/util/shared_buffer.h"
#include "mongo/util/string_map.h"
#include "third_party/murmurhash3/MurmurHash3.h"

namespace mongo {
namespace {
SharedBuffer readFile(std::string filename) {
    std::ifstream ifs(filename, std::ios::binary | std::ios::ate);
    std::ifstream::pos_type pos = ifs.tellg();

    auto result = SharedBuffer::allocate(pos);
    log() << "readFile " << filename << " of length " << (long long)pos;
    invariant(pos >= 0);
    log() << "resized";

    ifs.seekg(0, std::ios::beg);
    log() << "seeked";
    ifs.read(result.get(), pos);
    log() << "finished reading";
    return result;
}

int splitBSON(int argc, const char* argv[]) {
    std::string fieldname = argv[1];
    std::string key = argv[2];
    LRUCache<uint32_t, bool> cache(std::stoi(argv[3]));
    std::vector<uint32_t> schemas;
    int64_t misses = 0;
    for (int j = 4; j < argc; j++) {
        size_t totalSchemaSize = 0;
        size_t totalDataSize = 0;
        size_t totalSplitSize = 0;
        std::string filename = argv[j];
        auto data = readFile(filename);
        log() << "starting search for docs where " << fieldname << " starts with " << key;

        int occurrences = 0;
        int docs = 0;
        int runs = 0;
        std::string::size_type pos = 0;
        using SchemaMap = StringMap<uint32_t>;
        SchemaMap schemaCount;
        uint32_t lastHash = 0;
        while (pos + 4 < data.capacity()) {
            auto obj = BSONObj(data.get() + pos);
            BSONElement field = obj[fieldname];
            occurrences +=
                (field.type() == String && field.checkAndGetStringData().startsWith(key));
            SplitBSONBuilder builder;
            builder.appendElements(obj);
            totalDataSize += obj.objsize();
            totalSplitSize += builder.dataSize();
            auto hash = builder.hash();
            SplitBSON split = builder.release();
            BSONObj splitObj = split.obj();
            invariant(obj.objsize() == splitObj.objsize());
            invariant(!memcmp(obj.objdata(), splitObj.objdata(), obj.objsize()));

            schemas.push_back(hash);
            {
                auto it = cache.find(hash);
                if (it == cache.end()) {
                    misses++;
                    cache.add(hash, true);
                }
            }
            auto p = schemaCount.try_emplace(StringData(split.schema(), split.schemaSize()));
            auto it = p.first;
            if (p.second) {
                totalSchemaSize += split.schemaSize();
                ++(it->second);
            } else {
                it->second = 1;
            }

            if (lastHash == hash)
                runs++;
            lastHash = hash;
            ++docs;
            pos += obj.objsize();
            invariant(pos <= data.capacity());
        }
        {
            std::ofstream ofs("schema-count");
            for (auto elem : schemaCount) {
                int count = elem.second;
                ofs << "schema count " << count << "\n";
            }
        }
        {
            std::ofstream ofs("schema-trace");
            for (auto elem : schemas) {
                ofs << elem << "\n";
            }
        }

        log() << filename << " has " << docs << " docs, " << occurrences << " of which have "
              << fieldname << " starting with " << key;
        log() << filename << " has data size " << totalDataSize << ", schema size "
              << totalSchemaSize << ", split data size " << totalSplitSize;
        log() << filename << " has " << schemaCount.size() << " different schemas";
        log() << filename << " has " << runs
              << " cases where the schema is unchanged in sequential docs";
        log() << filename << " had " << misses << " misses in cache of size " << cache.size()
              << ": " << ((docs - misses) * 100 / docs) << "% hit rate";
    }
    return 0;
}
}  // namespace
}  // namespace mongo

int main(int argc, const char* argv[]) {
    if (argc < 4) {
        mongo::severe() << "usage: " << argv[0] << " key file...";
        return -1;
    }
    return mongo::splitBSON(argc, argv);
}
