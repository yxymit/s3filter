//
// Created by matt on 11/12/19.
//

#include "normal/pushdown/AggregateExpression.h"

#include <utility>

AggregateExpression::AggregateExpression(std::shared_ptr<TupleSet> (*fn)(std::shared_ptr<TupleSet>, std::shared_ptr<TupleSet>)) {
  m_fn = fn;
}

std::shared_ptr<TupleSet> AggregateExpression::apply(std::shared_ptr<TupleSet> tupleSet, std::shared_ptr<TupleSet> aggregateTupleSet) {
  return m_fn(std::move(tupleSet), std::move(aggregateTupleSet));
}

