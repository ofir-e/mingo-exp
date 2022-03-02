import delay from 'delay';
import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';
import { customParseExpression, AsyncAggregator } from '../src/index';
import { v4 as uuidv4, validate } from 'uuid';
import Cache from 'memory-cache';

const { $customParse } = customParseExpression({
  testAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1 + num2;
  },
  uuidAsync: async () => {
    await delay(30);
    return uuidv4();
  }
});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as Record<string, PipelineOperator>);
useOperators(OperatorType.EXPRESSION, { $customParse });

const inputData = [
  { a: { b: { c: 3 }, d: 4, e: 5 } },
  { a: { b: { c: 6 }, d: 6, e: 6 } }
]

test('sync aggregation functions called', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        q: { $customParse: { type: 'concat', args: ['*', '$a.b.c', '$a.d', '$a.e'] } }
      }
    }
  ]);

  expect(await agg.run(inputData)).toEqual([
    { n1: 3, n2: 4, n3: 5, q: '3*4*5' },
    { n1: 6, n2: 6, n3: 6, q: '6*6*6' }
  ]);

})

test('async aggregation functions called', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        a1: { $customParse: { type: 'testAsync', args: ['$a.d', '$a.e'] } },
        a2: { $customParse: { type: 'testAsync', args: ['$a.b.c', '$a.d'] } }
      }
    },
    {
      $project: {
        a3: { $customParse: { type: 'testAsync', args: ['$a1', '$a2'] } },
      }
    }
  ]);

  expect(await agg.run(inputData)).toEqual([
    { a3: 16 },
    { a3: 24 }
  ]);

})

test('async aggregation functions called once for each pipeline using $runOnce', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        a1: { $runOnce: { $customParse: { type: 'uuidAsync' } } },
      }
    },
    {
      $project: {
        a1: '$a1',
        a2: { $customParse: { type: 'uuidAsync', args: ['$a1', 6] } },
      }
    }
  ]);
  const res = await agg.run(inputData);

  expect(Object.keys(res[0])).toEqual(['a1', 'a2']);
  expect(Object.keys(res[1])).toEqual(['a1', 'a2']);
  expect(res[0].a1).toEqual(res[1].a1);
  expect(res[0].a2).not.toEqual(res[1].a2);
  expect(validate(res[0].a1 as string)).toBeTruthy();
  expect(validate(res[0].a2 as string)).toBeTruthy();
  expect(validate(res[1].a2 as string)).toBeTruthy();
})

test('async aggregation functions called once for same pipeline+args using $runOnce', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        a1: { $runOnce: { $customParse: { type: 'uuidAsync', args: ['arg1'] } } },
        a3: { $runOnce: { $customParse: { type: 'uuidAsync', args: ['different arg'] } } },
      }
    },
    {
      $project: {
        a1: '$a1',
        a3: '$a3',
        a2: { $customParse: { type: 'uuidAsync', args: ['$a1', 6] } },
      }
    }
  ]);
  const res = await agg.run(inputData);

  expect(Object.keys(res[0])).toEqual(['a1', 'a3', 'a2']);
  expect(Object.keys(res[1])).toEqual(['a1', 'a3', 'a2']);
  expect(res[0].a1).not.toEqual(res[1].a3);
  expect(res[0].a3).toEqual(res[1].a3);
  expect(res[0].a1).toEqual(res[1].a1);
  expect(res[0].a2).not.toEqual(res[1].a2);
  expect(validate(res[0].a1 as string)).toBeTruthy();
  expect(validate(res[0].a2 as string)).toBeTruthy();
  expect(validate(res[0].a3 as string)).toBeTruthy();
  expect(validate(res[1].a2 as string)).toBeTruthy();
})

test('async aggregation adds pipelineId for each pipe', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        p1: '$pipelineId',
      }
    },
    {
      $project: {
        p1: '$p1',
        p2: '$pipelineId',
      }
    }
  ]);

  const res = await agg.run(inputData);

  expect(Object.keys(res[0])).toEqual(['p1', 'p2']);
  expect(Object.keys(res[1])).toEqual(['p1', 'p2']);
  expect(res[0].p1).toEqual(res[1].p1);
  expect(res[0].p2).toEqual(res[1].p2);
  expect(res[0].p1).not.toEqual(res[0].p2);

})

afterAll(function () {
  Cache.clear();
})