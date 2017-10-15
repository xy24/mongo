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

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/util/hex.h"
#include "mongo/util/log.h"
#include "mongo/util/shared_buffer.h"
#include "mongo/util/string_map.h"
#include "third_party/murmurhash3/MurmurHash3.h"

namespace mongo {
class SplitBSONBuilder {

public:
    SplitBSONBuilder(int initSchemaSize = 512, int initFixedSize = 512, int initVarSize = 0)
        : _sb(_schema),
          _schema(initSchemaSize),
          _fb(_fixed),
          _fixed(initFixedSize),
          _vb(_var),
          _var(initVarSize) {
        // Skip over space for the schema length and the fixed length, which are filled in
        // by _done.
        _sb.skip(2 * sizeof(int32_t));
        // Reserve space for the EOO byte. This means _done() can't fail.
        _sb.reserveBytes(1);
    }
    void append(StringData fieldName, double val) {
        _sb.appendNum(static_cast<char>(NumberDouble));
        _sb.appendStr(fieldName);
        _fb.appendNum(val);
    }
    void append(StringData fieldName, int val) {
        _sb.appendNum(static_cast<char>(NumberInt));
        _sb.appendStr(fieldName);
        _fb.appendNum(val);
    }
    void append(StringData fieldName, long long val) {
        _sb.appendNum(static_cast<char>(NumberLong));
        _sb.appendStr(fieldName);
        _fb.appendNum(val);
    }
    void append(StringData fieldName, StringData val) {
        _sb.appendNum(static_cast<char>(String));
        _sb.appendStr(fieldName);
        _vb.appendStr(val, /*includeEOO*/ false);
        _fb.appendNum(_vb.len());
    }
    void appendElements(const BSONObj& x) {
        for (const BSONElement& elem : x) {
            switch (elem.type()) {
                case NumberDouble:
                    append(elem.fieldNameStringData(), elem._numberDouble());
                    break;
                case String:
                    append(elem.fieldNameStringData(), elem.valueStringData());
                    break;
                case jstOID:
                case NumberInt:
                    append(elem.fieldNameStringData(), elem._numberInt());
                    break;
                case NumberLong:
                    append(elem.fieldNameStringData(), elem._numberLong());
                    break;
                case EOO:
                    _sb.claimReservedBytes(1);
                    _sb.appendNum(static_cast<char>(EOO));
                    return;

                default:
                    std::string msg = str::stream() << "field " << elem.fieldNameStringData()
                                                    << " has unsupported type "
                                                    << typeName(elem.type());
                    log() << msg;
                    uasserted(ErrorCodes::UnsupportedFormat, msg);
            }
        }
        LOG(1) << x.objsize() << " BSON bytes => " << _sb.len() << " schema + " << _fb.len()
               << " fixed + " << _vb.len() << " variable length bytes";
    }

    uint32_t hash() {
        uint32_t result;
        MurmurHash3_x86_32(_sb.buf(), _sb.len(), 0, &result);
        return result;
    }

    StringData schema() {
        return StringData(_sb.buf(), _sb.len());
    }

private:
    BufBuilder& _sb;
    BufBuilder _schema;
    BufBuilder& _fb;
    BufBuilder _fixed;
    BufBuilder& _vb;
    BufBuilder _var;
};

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
    for (int j = 3; j < argc; j++) {
        std::string filename = argv[j];
        auto data = readFile(filename);
        log() << "starting search for docs where " << fieldname << " starts with " << key;

        int occurrences = 0;
        int docs = 0;
        int runs = 0;
        std::string::size_type pos = 0;
        using SchemaMap = StringMap<int>;
        SchemaMap schemaCount;
        SchemaMap::iterator lastIt = schemaCount.end();
        while (pos + 4 < data.capacity()) {
            auto obj = BSONObj(data.get() + pos);
            BSONElement field = obj[fieldname];
            occurrences +=
                (field.type() == String && field.checkAndGetStringData().startsWith(key));
            SplitBSONBuilder builder;
            builder.appendElements(obj);
            auto it = schemaCount.try_emplace(builder.schema()).first;
            ++(it->second);
            if (lastIt == it)
                runs++;
            lastIt = it;
            ++docs;
            pos += obj.objsize();
            invariant(pos <= data.capacity());
        }
        for (auto elem : schemaCount) {
            int count = elem.second;
            log() << "schema count " << count;
        }

        log() << filename << " has " << docs << " docs, " << occurrences << " of which have "
              << fieldname << " starting with " << key;
        log() << filename << " has " << runs
              << " cases where the schema is unchanged in sequential docs";
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
