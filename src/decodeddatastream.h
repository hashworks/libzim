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

#ifndef ZIM_DECODECDATASTREAM_H
#define ZIM_DECODECDATASTREAM_H

#include "compression.h"
#include "idatastream.h"

namespace zim
{

template<typename Decoder>
class DecodedDataStream : public IDataStream
{
private: // constants
  enum { BUF_SIZE = 1024 };

public: // functions
  DecodedDataStream(IDataStream* inputData, size_t inputSize)
    : encodedDataStream_(*inputData)
    , inputBytesLeft_(inputSize)
    , encodedDataBuf_(BUF_SIZE)
  {
    readNextChunk();
    Decoder::init_stream_decoder(&decoderState_, (char*)encodedDataBuf_.data());
    decoderState_.next_in  = encodedDataBuf_.data();
    decoderState_.avail_in = encodedDataBuf_.size();
  }

private: // functions
  void readNextChunk()
  {
    const size_t n = std::min(size_t(BUF_SIZE), inputBytesLeft_);
    encodedDataStream_.read(encodedDataBuf_.data(), n);
    inputBytesLeft_ -= n;
    if ( inputBytesLeft_ == 0 )
      encodedDataBuf_.resize(n);

    decoderState_.next_in  = encodedDataBuf_.data();
    decoderState_.avail_in = encodedDataBuf_.size();
  }

  CompStatus decodeMoreBytes()
  {
    CompStep step = CompStep::STEP;
    if ( decoderState_.avail_in == 0 )
    {
      if ( inputBytesLeft_ == 0 )
        step = CompStep::FINISH;
      else
        readNextChunk();
    }

    return Decoder::stream_run_decode(&decoderState_, step);
  }

  void readImpl(void* buf, size_t nbytes) override
  {
    decoderState_.next_out = (unsigned char*)buf;
    decoderState_.avail_out = nbytes;
    while ( decoderState_.avail_out != 0 )
    {
      decodeMoreBytes();
    }
  }

private: // types
  typedef typename Decoder::stream_t DecoderState;

private: // data
  IDataStream& encodedDataStream_;
  size_t inputBytesLeft_; // count of bytes left in the input stream
  DecoderState decoderState_;
  std::vector<unsigned char> encodedDataBuf_;
};

} // namespace zim

#endif // ZIM_DECODECDATASTREAM_H
