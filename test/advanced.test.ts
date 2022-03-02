import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';
import { AsyncAggregator, generateCustomOperator } from '../src/index';
import Cache from 'memory-cache';
import delay from 'delay';
import _ from 'lodash';

// simplified custom operator
const { $sendToServerInBatchesForPerformance, $lodashFind } = generateCustomOperator({
  $sendToServerInBatchesForPerformance: async (ids: string[]) => {
    // in real usage this can be a request to a server
    await delay(30);
    return ids.map(id => ({ id, name: `king number ${id}` }));
  },
  $lodashFind: (arr: any[], predicate: any, pathInMatch: string = '') => {
    return _.get(_.find(arr, predicate), pathInMatch);
  }
});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as Record<string, PipelineOperator>);
useOperators(OperatorType.EXPRESSION, { $lodashFind, $sendToServerInBatchesForPerformance });

const inputData = [
  { a: { b: '123' } },
  { a: { b: '321' } }
]

test('collect can be called for multiple fields ', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        id: '$a.b',
        idsForBatch: { $collect: '$a.b' }
      }
    },
    {
      $project: {
        id: '$id',
        response: { $runOnce: { $sendToServerInBatchesForPerformance: ['$idsForBatch'] } }
      }
    },
    {
      $project: {
        id: '$id',
        name: { $lodashFind: ['$response', { id: '$id' }, 'name'] }
      }
    }
  ]);

  expect(await agg.run(inputData)).toEqual([{ id: '123', name: 'king number 123' }, { id: '321', name: 'king number 321' }]);

})

afterAll(function () {
  Cache.clear();
})
