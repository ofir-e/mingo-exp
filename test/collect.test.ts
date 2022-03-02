import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';
import { AsyncAggregator } from '../src/index';
import Cache from 'memory-cache';


// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as Record<string, PipelineOperator>);

const inputData = [
  { a: { b: 1, c: 6 } },
  { a: { b: 2, c: 7 } },
  { a: { c: 6 } },
  { a: { b: 3, c: 8 } },
]

test('collect can be called for multiple fields ', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        bs: { $collect: '$a.b' },
        cs: { $collect: '$a.c' },
        b: '$a.b',
        c: '$a.c',
      }
    },
    {
      $project: {
        bs2: { $collect: '$b' },
        cs2: { $collect: '$c' },
        b: '$b',
        c: '$c',
        bs: '$bs',
        cs: '$cs',
      }
    }
  ]);

  expect(await agg.run(inputData)).toEqual([
    { b: 1, c: 6, bs: [1, 2, undefined, 3], cs: [6, 7, 6, 8], bs2: [1, 2, undefined, 3], cs2: [6, 7, 6, 8] },
    { b: 2, c: 7, bs: [1, 2, undefined, 3], cs: [6, 7, 6, 8], bs2: [1, 2, undefined, 3], cs2: [6, 7, 6, 8] },
    { c: 6, bs: [1, 2, undefined, 3], cs: [6, 7, 6, 8], bs2: [1, 2, undefined, 3], cs2: [6, 7, 6, 8] },
    { b: 3, c: 8, bs: [1, 2, undefined, 3], cs: [6, 7, 6, 8], bs2: [1, 2, undefined, 3], cs2: [6, 7, 6, 8] }
  ]);

})

afterAll(function () {
  Cache.clear();
})


