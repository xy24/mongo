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

#include "mongo/bson/bsonobj.h"
#include "mongo/util/log.h"
#include "mongo/util/shared_buffer.h"

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
    for (int j = 3; j < argc; j++) {
        std::string filename = argv[j];
        auto data = readFile(filename);
        log() << "starting search for docs where " << fieldname << " starts with " << key;

        int occurrences = 0;
        std::string::size_type pos = 0;
        while (pos + 4 < data.capacity()) {
            auto obj = BSONObj(data.get() + pos);
            BSONElement field = obj[fieldname];
            occurrences +=
                (field.type() == String && field.checkAndGetStringData().startsWith(key));
            pos += obj.objsize();
            invariant(pos <= data.capacity());
        }

        log() << filename << " has " << occurrences << " docs where " << fieldname
              << " starts with " << key;
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
