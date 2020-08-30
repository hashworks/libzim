/*
 * Copyright (C) 2020 Veloman Yunkan
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

#include "decodeddatastream.h"
#include "bufdatastream.h"

#include "gtest/gtest.h"

namespace
{

using namespace zim;

template<class CompressionInfo>
IDataStream::Blob
compress(const std::string& data)
{
  zim::Compressor<CompressionInfo> compressor(data.size());
  compressor.init(const_cast<char*>(data.c_str()));
  compressor.feed(data.c_str(), data.size());
  zim::zsize_t comp_size;
  auto comp_data = compressor.get_data(&comp_size);
  return IDataStream::Blob(std::move(comp_data), comp_size.v);
}

std::string toString(const IDataStream::Blob& blob)
{
  return std::string(blob.data(), blob.size());
}

template<typename T>
class DecodedDataStreamTest : public testing::Test {
  protected:
    typedef T CompressionInfo;
};

using CompressionTypes = ::testing::Types<
  LZMA_INFO,
  ZSTD_INFO
#if defined(ENABLE_ZLIB)
  ,ZIP_INFO
#endif
>;

TYPED_TEST_CASE(DecodedDataStreamTest, CompressionTypes);

TYPED_TEST(DecodedDataStreamTest, shouldJustWork) {
  typedef typename TestFixture::CompressionInfo CompressionInfo;

  const int N = 10;
  const std::string s("DecodedDataStream should work correctly");
  std::string data;
  for (int i=0; i<N; i++)
    data += s;

  const auto compData = compress<CompressionInfo>(data);

  BufDataStream bds(compData.data(), compData.size());
  DecodedDataStream<CompressionInfo> dds(&bds, compData.size());
  for (int i=0; i<N; i++)
  {
    ASSERT_EQ(s, toString(dds.readBlob(s.size()))) << "i: " << i;
  }
}

} // unnamed namespace
