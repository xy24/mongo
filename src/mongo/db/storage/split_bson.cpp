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
#include "mongo/unittest/bson_test_util.h"
#include "mongo/util/hex.h"
#include "mongo/util/log.h"
#include "mongo/util/lru_cache.h"
#include "mongo/util/shared_buffer.h"
#include "mongo/util/string_map.h"
#include "third_party/murmurhash3/MurmurHash3.h"

namespace mongo {
class SchemaElement {
public:
    SchemaElement(const char* data) : _data(data){};
    BSONType type() const {
        const signed char typeByte = ConstDataView(_data).read<signed char>();
        return static_cast<BSONType>(typeByte);
    }
    bool eoo() const {
        return type() == EOO;
    }
    const StringData fieldName() const {
        if (type() == EOO)
            return {};

        size_t len = 0;
        const char* it = _data + 1;
        do {
            len = (len << 7) + (*it & 0x7f);
        } while (*it++ & ~0x7f);

        return StringData(it, len);
    }

    const char* rawdata() const {
        return _data;
    }
    int fixedSize() const {
        int size = 0;
        switch (type()) {
            case EOO:
            case Undefined:
            case jstNULL:
            case MaxKey:
            case MinKey:
                break;
            case mongo::Bool:
                size = 1;
                break;
            case NumberInt:
                size = 4;
                break;
            case bsonTimestamp:
            case mongo::Date:
            case NumberDouble:
            case NumberLong:
                size = 8;
                break;
            case NumberDecimal:
                size = 16;
                break;
            case jstOID:
                size = OID::kOIDSize;
                break;
            case Symbol:
            case Code:
            case mongo::String:
                size = 4;
                break;
            case DBRef:
                size = 4 + 12;
                break;
            case CodeWScope:
            case Object:
            case mongo::Array:
                invariant("not implemented yet");
                break;
            case BinData:
                size = 4 + 1 /*subtype*/;
                break;
            case RegEx:
                size = 4;
                break;
            default: {
                StringBuilder ss;
                ss << "SchemaElement: bad type " << (int)type();
                std::string msg = ss.str();
                uasserted(ErrorCodes::UnsupportedFormat, msg.c_str());
            }
        }
        return size;
    }

private:
    const char* _data;
};

class SplitBSONBuilder {

public:
    SplitBSONBuilder(int initSchemaSize = 512, int initFixedSize = 512, int initVarSize = 0)
        : _sb(_schema),
          _schema(initSchemaSize),
          _fb(_fixed),
          _fixed(initFixedSize),
          _vb(_var),
          _var(initVarSize) {
        // Skip over space for the schema length, the fixed data length and the variable data
        // length, which are filled in by _done().
        _sb.skip(2 * sizeof(int32_t));
        _fb.skip(sizeof(int32_t));
        // Reserve space for the EOO byte. This means _done() can't fail.
        // _sb.reserveBytes(1);
    }
    void appendFieldName(StringData fieldName) {
        size_t len = fieldName.size();
        do {
            _sb.appendNum(static_cast<char>(len % 0x80));
            len /= 0x80;
        } while (len);
        _sb.appendStr(fieldName, /*includeEOO*/ false);
    }

    void appendElements(const BSONObj& x) {
        for (const BSONElement& elem : x) {
            switch (elem.type()) {
                /* Fixed size data types */
                case NumberDouble:
                case jstOID:
                case Bool:
                case NumberInt:
                case Date:
                case jstNULL:
                case bsonTimestamp:
                case NumberLong:
                case NumberDecimal:
                    _sb.appendChar(*elem.rawdata());
                    appendFieldName(elem.fieldNameStringData());
                    _fb.appendBuf(elem.value(), elem.valuesize());
                    break;
                case String:
                    _sb.appendNum(static_cast<char>(String));
                    appendFieldName(elem.fieldNameStringData());
                    _vb.appendBuf(elem.valuestr(),
                                  elem.valuestrsize());  // includes terminating null
                    _fb.appendNum(endian::nativeToLittle(_vb.len()));
                    break;

                case EOO:
                    // _sb.claimReservedBytes(1);
                    _done();
                    return;

                default:
                    std::string msg = str::stream() << "field " << elem.fieldNameStringData()
                                                    << " has unsupported type "
                                                    << typeName(elem.type());
                    log() << msg;
                    uasserted(ErrorCodes::UnsupportedFormat, msg);
            }
        }
        _done();
        LOG(1) << x.objsize() << " BSON bytes => " << _sb.len() << " schema + " << _fb.len()
               << " fixed + " << _vb.len() << " variable length bytes";
    }

    uint32_t hash() {
        uint32_t result;
        invariant(_sb.buf()[_sb.len() - 1] == static_cast<char>(EOO));
        MurmurHash3_x86_32(_sb.buf(), _sb.len(), 0, &result);
        return result;
    }

    StringData schema() {
        invariant(_sb.buf()[_sb.len() - 1] == static_cast<char>(EOO));
        return StringData(_sb.buf(), _sb.len());
    }

    void toBSON(BufBuilder* builder, int s_ofs = 0, int f_ofs = 0, int v_ofs = 0) {
        builder->skip(sizeof(int32_t));

        // Read schema length and set up pointers to current and end positions in schema.
        const char* s_ptr = _sb.buf() + s_ofs;
        int s_len = ConstDataView(s_ptr).read<LittleEndian<int>>();
        s_ptr += sizeof(int);
        invariant(s_len <= _sb.len() - s_ofs);
        const char* s_end = s_ptr + s_len;

        // Read fixed data length and set up pointers.
        invariant(_fb.len() >= f_ofs);
        const char* f_ptr = _fb.buf() + f_ofs;
        int f_len = ConstDataView(s_ptr).read<LittleEndian<int>>();
        s_ptr += sizeof(int);
        invariant(f_len <= _fb.len() - f_ofs);
        const char* f_end = f_ptr + f_len;

        // Read variable data length and set up pointers.
        const char* v_ptr = _vb.buf() + v_ofs;
        int v_len = ConstDataView(f_ptr).read<LittleEndian<int>>();
        f_ptr += sizeof(int);
        invariant(v_len <= _vb.len() - v_ofs);
        const char* v_end = v_ptr + v_len;

        while (s_ptr < s_end && *s_ptr) {
            SchemaElement s_elem(s_ptr);
            switch (s_elem.type()) {
                case NumberDouble:
                case jstOID:
                case Bool:
                case NumberInt:
                case Date:
                case jstNULL:
                case bsonTimestamp:
                case NumberLong:
                case NumberDecimal: {
                    builder->appendChar(*s_ptr);
                    StringData fieldName = s_elem.fieldName();
                    int fixedSize = s_elem.fixedSize();
                    invariant(f_ptr + fixedSize <= f_end);
                    builder->appendStr(fieldName, /*includeEOO*/ true);
                    builder->appendBuf(f_ptr, fixedSize);

                    s_ptr = fieldName.end();
                    f_ptr += fixedSize;
                    invariant(s_ptr <= s_end);
                    break;
                }
                case String: {
                    // Append type byte and fieldName.
                    builder->appendChar(*s_ptr);
                    StringData fieldName = s_elem.fieldName();
                    builder->appendStr(fieldName, /*includeEOO*/ true);

                    // Append string size.
                    const int fixedSize = sizeof(int);
                    invariant(f_ptr + fixedSize <= f_end);
                    const int varSize =
                        _vb.buf() + ConstDataView(f_ptr).read<LittleEndian<int>>() - v_ptr;
                    builder->appendBuf(f_ptr, fixedSize);

                    // Append actual string (which includes its terminating null character).
                    invariant(v_ptr + varSize <= v_end);
                    builder->appendBuf(v_ptr, varSize);
                    s_ptr = fieldName.end();
                    f_ptr += fixedSize;
                    v_ptr += varSize;
                    invariant(s_ptr <= s_end);

                    break;
                }
                default:
                    std::string msg = str::stream() << "field " << s_elem.fieldName()
                                                    << " has unsupported type "
                                                    << typeName(s_elem.type());
                    log() << msg;
                    uasserted(ErrorCodes::UnsupportedFormat, msg);
            }
        }
        builder->appendChar(EOO);
        DataView(builder->buf()).write(tagLittleEndian(builder->len()));
    }

    BSONObj obj() {
        BufBuilder builder;
        toBSON(&builder);
        BSONObj out = BSONObj(builder.buf());
        out.shareOwnershipWith(builder.release());
        return out;
    }

private:
    void _done() {
        if (_sb.len() && _sb.buf()[_sb.len() - 1] == EOO)
            return;
        _sb.appendNum(static_cast<char>(EOO));
        uint32_t schemaSize = endian::nativeToLittle(_sb.len());
        uint32_t fixedSize = endian::nativeToLittle(_fb.len());
        memcpy(_sb.buf(), &schemaSize, sizeof(uint32_t));
        memcpy(_sb.buf() + sizeof(uint32_t), &fixedSize, sizeof(uint32_t));
        uint32_t variableSize = endian::nativeToLittle(_vb.len());
        memcpy(_fb.buf(), &variableSize, sizeof(uint32_t));
    }

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
    LRUCache<uint32_t, bool> cache(std::stoi(argv[3]));
    std::vector<uint32_t> schemas;
    int64_t misses = 0;
    for (int j = 4; j < argc; j++) {
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
            BSONObj splitObj = builder.obj();
            uassert(ErrorCodes::BadValue,
                    str::stream() << obj << " != " << splitObj,
                    obj.objsize() == splitObj.objsize());
            auto hash = builder.hash();
            schemas.push_back(hash);
            {
                auto it = cache.find(hash);
                if (it == cache.end()) {
                    misses++;
                    cache.add(hash, true);
                }
            }
            auto p = schemaCount.try_emplace(builder.schema());
            auto it = p.first;
            if (p.second)
                ++(it->second);
            else
                it->second = 0;
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
