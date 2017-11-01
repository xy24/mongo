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

#include <fstream>

#include "mongo/base/data_view.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/util/builder.h"
#include "mongo/util/shared_buffer.h"
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

class SplitBSON {
public:
    SplitBSON(SharedBuffer schema, const char* data) : _ownedSchema(schema), _data(data) {}
    SplitBSON(SharedBuffer schema, SharedBuffer data)
        : _ownedSchema(schema), _data(data.get()), _ownedData(data) {}
    BSONObj obj() {
        BufBuilder builder;
        toBSON(&builder);
        BSONObj out = BSONObj(builder.buf());
        out.shareOwnershipWith(builder.release());
        return out;
    }
    void toBSON(BufBuilder* builder, int s_ofs = 0, int f_ofs = 0, int v_ofs = 0);

    int schemaSize() {
        return ConstDataView(schema()).read<LittleEndian<int>>();
    }

    const char* schema() {
        return _ownedSchema.get();
    }

    const char* data() {
        return _data;
    }

    size_t dataSize() {
        size_t fixedSize = ConstDataView(schema() + sizeof(uint32_t)).read<LittleEndian<size_t>>();
        size_t varSize = ConstDataView(data()).read<LittleEndian<size_t>>();

        return fixedSize + varSize;
    }

private:
    SharedBuffer _ownedSchema;
    const char* _data;
    SharedBuffer _ownedData;
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

    void appendElements(const BSONObj& x);

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

    int dataSize() {
        return _fb.len() + _vb.len();
    }

    SplitBSON release() {
        _fb.appendBuf(_vb.buf(), _vb.len());
        _vb.reset();

        return {_sb.release(), _fb.release() };
    }

private:
    void _done() {
        if (_sb.len() && _sb.buf()[_sb.len() - 1] == EOO)
            return;
        _sb.appendNum(static_cast<char>(EOO));
        // Fill in the skipped length fields for schema, fixed size and variable size data.
        DataView(_sb.buf()).write(tagLittleEndian(_sb.len()));
        DataView(_sb.buf() + sizeof(int)).write(tagLittleEndian(_fb.len()));
        DataView(_fb.buf()).write(tagLittleEndian(_vb.len()));
    }

    BufBuilder& _sb;
    BufBuilder _schema;
    BufBuilder& _fb;
    BufBuilder _fixed;
    BufBuilder& _vb;
    BufBuilder _var;
};

}  // namespace mongo
