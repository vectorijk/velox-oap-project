/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/exec/RowContainer.h"
#include "velox/vector/BaseVector.h"

/// Simple WindowPartition that builds over the RowContainer used for storing
/// the input rows in the Window Operator. This works completely in-memory.
/// WindowPartition supports partial window partitioning to facilitate
/// RowsStreamingWindowBuild, which means that subsequent calculations within
/// the WindowPartition do not need to wait until the current partition is fully
/// ready before commencing. Calculations can begin as soon as a portion of the
/// rows are ready.
/// TODO: This implementation will be revised for Spill to disk semantics.

namespace facebook::velox::exec {

class WindowPartition {
 public:
  /// The WindowPartition is used by the Window operator and WindowFunction
  /// objects to access the underlying data and columns of a partition of rows.
  /// The WindowPartition is constructed by WindowBuild from the input data.
  /// 'data' : Underlying RowContainer of the WindowBuild.
  /// 'rows' : Pointers to rows in the RowContainer belonging to this partition.
  /// 'inputMapping' : Mapping from Window input column to the column position
  /// in 'data' for it. This is required because the WindowBuild re-orders
  /// the columns in 'data' for use with the spiller.
  /// 'sortKeyInfo' : Order by columns used by the the Window operator. Used to
  /// get peer rows from the input partition.
  WindowPartition(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo);

  /// The WindowPartition is used for partial partition when the input data will
  /// be a subset of the entire partition.
  WindowPartition(
      RowContainer* data,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo);

  /// Adds remaining input rows when building the partial WindowPartition.
  void addRows(const std::vector<char*>& rows);

  /// Clear the processed rows fow partial WindowPartition.
  void clearOutputRows(vector_size_t numRows);

  /// Returns the number of rows in the current WindowPartition.
  vector_size_t numRows() const {
    return partition_.size();
  }

  /// Returns the number of rows that will be processed.
  vector_size_t numRowsForProcessing() const {
    if (startRow_ > 0) {
      return partition_.size() - 1;
    }
    return partition_.size();
  }

  bool isComplete() const {
    return complete_;
  }

  bool isPartial() const {
    return partial_;
  }

  void setComplete() {
    complete_ = true;
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the rows at positions in the 'rowNumbers'
  /// array from the partition input data.
  void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for 'numRows' starting at positions 'partitionOffset'
  /// in the partition input data.
  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Extracts null positions at 'columnIndex' into 'nullsBuffer' for
  /// 'numRows' starting at positions 'partitionOffset' in the partition
  /// input data.
  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  /// Extracts null positions at 'col' into 'nulls'. The null positions
  /// are from the smallest 'frameStarts' value to the greatest 'frameEnds'
  /// value for 'validRows'. Both 'frameStarts' and 'frameEnds' are buffers
  /// of type vector_size_t.
  /// The returned value is an optional pair of vector_size_t.
  /// The pair is returned only if null values are found in the nulls
  /// extracted. The first value of the pair is the smallest frameStart for
  /// nulls. The second is the number of frames extracted.
  std::optional<std::pair<vector_size_t, vector_size_t>> extractNulls(
      column_index_t col,
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      BufferPtr* nulls) const;

  /// Sets in 'rawPeerStarts' and in 'rawPeerEnds' the peer start and peer end
  /// offsets of the rows between 'start' and 'end' of the current partition.
  /// 'peer' row are all the rows having the same value of the order by columns
  /// as the current row.
  /// computePeerBuffers is called multiple times for each partition. It is
  /// called in sequential order of start to end rows.
  /// The peerStarts/peerEnds of the startRow could be same as the last row in
  /// the previous call to computePeerBuffers (if they have the same order by
  /// keys). So peerStart and peerEnd of the last row of this call are returned
  /// to be passed as prevPeerStart and prevPeerEnd to the subsequent
  /// call to computePeerBuffers.
  std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds);

  /// Sets in 'rawFrameBounds' the frame boundary for the k range
  /// preceding/following frame.
  /// @param isStartBound start or end boundary of the frame.
  /// @param isPreceding preceding or following boundary.
  /// @param frameColumn column which has the range boundary for that row.
  /// @param startRow starting row in the partition for this buffer computation.
  /// @param numRows number of rows to compute buffer for.
  /// @param rawPeerStarts buffer of peer row values for each row. If the frame
  /// column is null, then its peer row value is the frame boundary.
  void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds) const;

 private:
  bool compareRowsWithSortKeys(const char* lhs, const char* rhs) const;

  vector_size_t findPeerGroupEndIndex(
      vector_size_t currentStart,
      vector_size_t lastRow,
      std::function<bool(const char*, const char*)> peerCompare);

  // Searches for 'currentRow[frameColumn]' in 'orderByColumn' of rows between
  // 'start' and 'end' in the partition. 'firstMatch' specifies if first or last
  // row is matched.
  vector_size_t searchFrameValue(
      bool firstMatch,
      vector_size_t start,
      vector_size_t end,
      vector_size_t currentRow,
      column_index_t orderByColumn,
      column_index_t frameColumn,
      const CompareFlags& flags) const;

  vector_size_t linearSearchFrameValue(
      bool firstMatch,
      vector_size_t start,
      vector_size_t end,
      vector_size_t currentRow,
      column_index_t orderByColumn,
      column_index_t frameColumn,
      const CompareFlags& flags) const;

  // Iterates over 'numBlockRows' and searches frame value for each row.
  void updateKRangeFrameBounds(
      bool firstMatch,
      bool isPreceding,
      const CompareFlags& flags,
      vector_size_t startRow,
      vector_size_t numRows,
      column_index_t frameColumn,
      const vector_size_t* rawPeerBounds,
      vector_size_t* rawFrameBounds) const;

  // Returns the starting offset of the current partial window partition within
  // the full partition.
  vector_size_t startRow() const {
    return startRow_;
  }

  // The RowContainer associated with the partition.
  // It is owned by the WindowBuild that creates the partition.
  RowContainer* data_;

  // Holds input rows within the partial partition.
  std::vector<char*> rows_;

  // folly::Range is for the partition rows iterator provided by the
  // Window operator. The pointers are to rows from a RowContainer owned
  // by the operator. We can assume these are valid values for the lifetime
  // of WindowPartition.
  folly::Range<char**> partition_;

  // Indicates that the partial window partitioning process has been completed.
  bool complete_ = false;

  // Indicates partial window partition.
  bool partial_ = false;

  // Mapping from window input column -> index in data_. This is required
  // because the WindowBuild reorders data_ to place partition and sort keys
  // before other columns in data_. But the Window Operator and Function code
  // accesses WindowPartition using the indexes of Window input type.
  const std::vector<column_index_t> inputMapping_;

  // ORDER BY column info for this partition.
  const std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;

  // Copy of the input RowColumn objects that are used for
  // accessing the partition row columns. These RowColumn objects
  // index into RowContainer data_ above and can retrieve the column values.
  // The order of these columns is the same as that of the input row
  // of the Window operator. The WindowFunctions know the
  // corresponding indexes of their input arguments into this vector.
  // They will request for column vector values at the respective index.
  std::vector<exec::RowColumn> columns_;

  // The partition offset of the first row in rows_.
  vector_size_t startRow_ = 0;
};
} // namespace facebook::velox::exec
