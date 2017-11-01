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

#include "split_bson.h"

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/util/hex.h"
#include "mongo/util/log.h"
#include "mongo/util/lru_cache.h"
#include "mongo/util/shared_buffer.h"
#include "third_party/murmurhash3/MurmurHash3.h"

namespace mongo {
    void SplitBSONBuilder::toBSON(BufBuilder* builder, int s_ofs, int f_ofs, int v_ofs) {
        invariant(_sb.buf()[_sb.len() - 1] == static_cast<char>(EOO));
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
                    builder->appendNum(endian::nativeToLittle(varSize));

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
    void SplitBSONBuilder::appendElements(const BSONObj& x) {
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

}  // namespace mongo
