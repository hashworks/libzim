/*
 * Copyright (C) 2009 Tommi Maekitalo
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * is provided AS IS, WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, and
 * NON-INFRINGEMENT.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 */

#include "cluster.h"
#include <zim/blob.h>
#include <zim/error.h>
#include "file_reader.h"
#include "endian_tools.h"
#include "readerdatastreamwrapper.h"
#include <algorithm>
#include <stdlib.h>
#include <sstream>

#include "compression.h"
#include "log.h"

#include "config.h"

log_define("zim.cluster")

#define log_debug1(e)

namespace zim
{

namespace
{

std::shared_ptr<const Buffer>
getClusterBuffer(const Reader& zimReader, offset_t offset, CompressionType comp)
{
  zsize_t uncompressed_size(0);
  std::unique_ptr<char[]> uncompressed_data;
  switch (comp) {
    case zimcompLzma:
      uncompressed_data = uncompress<LZMA_INFO>(&zimReader, offset, &uncompressed_size);
      break;
    case zimcompZip:
#if defined(ENABLE_ZLIB)
      uncompressed_data = uncompress<ZIP_INFO>(&zimReader, offset, &uncompressed_size);
#else
      throw std::runtime_error("zlib not enabled in this library");
#endif
      break;
    case zimcompZstd:
      uncompressed_data = uncompress<ZSTD_INFO>(&zimReader, offset, &uncompressed_size);
      break;
    default:
      throw std::logic_error("compressions should not be something else than zimcompLzma, zimComZip or zimcompZstd.");
  }
  return std::make_shared<MemoryBuffer>(std::move(uncompressed_data), uncompressed_size);
}

std::unique_ptr<const Reader>
getClusterReader(const Reader& zimReader, offset_t offset, CompressionType* comp, bool* extended)
{
  uint8_t clusterInfo = zimReader.read(offset);
  *comp = static_cast<CompressionType>(clusterInfo & 0x0F);
  *extended = clusterInfo & 0x10;

  switch (*comp) {
    case zimcompDefault:
    case zimcompNone:
      {
      // No compression, just a sub_reader
        return zimReader.sub_reader(offset+offset_t(1));
      }
      break;
    case zimcompLzma:
    case zimcompZip:
    case zimcompZstd:
      {
        auto buffer = getClusterBuffer(zimReader, offset+offset_t(1), *comp);
        return std::unique_ptr<Reader>(new BufferReader(buffer));
      }
      break;
    case zimcompBzip2:
      throw std::runtime_error("bzip2 not enabled in this library");
    default:
      throw ZimFileFormatError("Invalid compression flag");
  }
}

} // unnamed namespace

////////////////////////////////////////////////////////////////////////////////
// Cluster
////////////////////////////////////////////////////////////////////////////////

  std::shared_ptr<Cluster> Cluster::read(const Reader& zimReader, offset_t clusterOffset)
  {
    CompressionType comp;
    bool extended;
    std::shared_ptr<const Reader> reader = getClusterReader(zimReader, clusterOffset, &comp, &extended);
    return (comp == zimcompDefault || comp == zimcompNone)
         ? std::make_shared<Cluster>(reader, extended)
         : std::make_shared<CompressedCluster>(reader, comp, extended);
  }

  Cluster::Cluster(std::shared_ptr<const Reader> reader_, bool isExtended)
    : isExtended(isExtended),
      reader(reader_),
      startOffset(0)
  {
    auto d = reader->offset();
    if (isExtended) {
      startOffset = read_header<uint64_t>();
    } else {
      startOffset = read_header<uint32_t>();
    }
    reader = reader->sub_reader(startOffset, zsize_t(offsets.back().v));
    auto d1 = reader->offset();
    ASSERT(d+startOffset, ==, d1);
  }

  /* This return the number of char read */
  template<typename OFFSET_TYPE>
  offset_t Cluster::read_header()
  {
    // read first offset, which specifies, how many offsets we need to read
    OFFSET_TYPE offset = reader->read_uint<OFFSET_TYPE>(offset_t(0));

    size_t n_offset = offset / sizeof(OFFSET_TYPE);
    const offset_t data_address(offset);

    // read offsets
    offsets.clear();
    offsets.reserve(n_offset);
    offsets.push_back(offset_t(0));

    auto buffer = reader->get_buffer(offset_t(0), zsize_t(offset));
    offset_t current = offset_t(sizeof(OFFSET_TYPE));
    while (--n_offset)
    {
      OFFSET_TYPE new_offset = buffer->as<OFFSET_TYPE>(current);
      ASSERT(new_offset, >=, offset);
      ASSERT(new_offset, <=, reader->size().v);

      offset = new_offset;
      offsets.push_back(offset_t(offset - data_address.v));
      current += sizeof(OFFSET_TYPE);
    }
    return data_address;
  }

  Blob Cluster::getBlob(blob_index_t n) const
  {
    if (n < count()) {
      auto blobSize = getBlobSize(n);
      if (blobSize.v > SIZE_MAX) {
        return Blob();
      }
      auto buffer = reader->get_buffer(offsets[blob_index_type(n)], blobSize);
      return Blob(buffer);
    } else {
      return Blob();
    }
  }

  Blob Cluster::getBlob(blob_index_t n, offset_t offset, zsize_t size) const
  {
    if (n < count()) {
      const auto blobSize = getBlobSize(n);
      if ( offset.v > blobSize.v ) {
        return Blob();
      }
      size = std::min(size, zsize_t(blobSize.v-offset.v));
      if (size.v > SIZE_MAX) {
        return Blob();
      }
      offset += offsets[blob_index_type(n)];
      auto buffer = reader->get_buffer(offset, size);
      return Blob(buffer);
    } else {
      return Blob();
    }
  }

////////////////////////////////////////////////////////////////////////////////
// CompressedCluster
////////////////////////////////////////////////////////////////////////////////

namespace
{

class IDSBlobBuffer : public Buffer
{
  IDataStream::Blob blob_;
  size_t offset_;
  size_t size_;

public:
  IDSBlobBuffer(const IDataStream::Blob& blob, size_t offset, size_t size)
    : Buffer(zsize_t(size))
    , blob_(blob)
    , offset_(offset)
    , size_(size)
  {
    ASSERT(offset_, <, blob_.size());
    ASSERT(offset_+size_, <=, blob_.size());
  }

  const char* dataImpl(offset_t offset) const
  {
    return blob_.data() + offset_ + offset.v;
  }
};

Blob idsBlob2zimBlob(const IDataStream::Blob& blob, size_t offset, size_t size)
{
  return Blob(std::make_shared<IDSBlobBuffer>(blob, offset, size));
}

} // unnamed namespace

CompressedCluster::CompressedCluster(std::shared_ptr<const Reader> reader, CompressionType comp, bool isExtended)
  : Cluster(isExtended)
  , compression_(comp)
{
  ASSERT(compression_, >, zimcompNone);

  ReaderDataStreamWrapper rdsw(reader.get());

  if ( isExtended )
    readHeader<uint64_t>(rdsw);
  else
    readHeader<uint32_t>(rdsw);

  readBlobs(rdsw);
}

bool
CompressedCluster::isCompressed() const
{
  return true;
}

CompressionType
CompressedCluster::getCompression() const
{
  return compression_;
}

offset_t
CompressedCluster::getBlobOffset(blob_index_t n) const
{
  throw std::logic_error("CompressedCluster::getBlobOffset() should never be called");
}

template<typename OFFSET_TYPE>
void
CompressedCluster::readHeader(IDataStream& ds)
{
  startOffset = offset_t(ds.read<OFFSET_TYPE>());

  size_t n_offset = startOffset.v / sizeof(OFFSET_TYPE);

  // read offsets
  offsets.clear();
  offsets.reserve(n_offset);
  offsets.push_back(offset_t(0));

  OFFSET_TYPE offset = startOffset.v;
  while (--n_offset)
  {
    OFFSET_TYPE new_offset = ds.read<OFFSET_TYPE>();
    ASSERT(new_offset, >=, offset);

    offset = new_offset;
    offsets.push_back(offset_t(offset - startOffset.v));
  }
}

void
CompressedCluster::readBlobs(IDataStream& ds)
{
  const size_t n = count().v;
  for ( size_t i = 0; i < n; ++i )
    blobs_.push_back(ds.readBlob(getBlobSize(blob_index_t(i)).v));
}

Blob
CompressedCluster::getBlob(blob_index_t n) const
{
  ASSERT(n.v, <, blobs_.size());
  const IDataStream::Blob& blob = blobs_[n.v];
  return idsBlob2zimBlob(blob, 0, blob.size());
}

Blob
CompressedCluster::getBlob(blob_index_t n, offset_t offset, zsize_t size) const
{
  ASSERT(n.v, <, blobs_.size());
  return idsBlob2zimBlob(blobs_[n.v], offset.v, size.v);
}

} // namespace zim
