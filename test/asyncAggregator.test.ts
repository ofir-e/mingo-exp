import delay from "delay";
import { useOperators, OperatorType } from "mingo/core";
import { $project } from "mingo/operators/pipeline";
import { AsyncAggregator } from '../src/asyncAggregator';
import { customParseExpression } from '../src/index';

const { $customParse } = customParseExpression({
  testAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1 + num2;
  }
});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as any);
useOperators(OperatorType.EXPRESSION, { $customParse } as any);

const inputData = [
  { a: { b: { c: 3 }, d: 4, e: 5 } },
  { a: { b: { c: 6 }, d: 6, e: 6 } }
]

test("sync aggregation functions called", async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        q: { $customParse: { type: "concat", args: ['*', '$a.b.c', '$a.d', '$a.e'] } }
      }
    }
  ]);

  expect(await agg.run(inputData)).toEqual([
    { n1: 3, n2: 4, n3: 5, q: '3*4*5' },
    { n1: 6, n2: 6, n3: 6, q: '6*6*6' }
  ]);

})

test("async aggregation functions called", async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        a1: { $customParse: { type: "testAsync", args: ['$a.d', '$a.e'] } },
        a2: { $customParse: { type: "testAsync", args: ['$a.b.c', '$a.d'] } }
      }
    },
    {
      $project: {
        a3: { $customParse: { type: "testAsync", args: ['$a1', '$a2'] } },
      }
    }
  ]);

  expect(await agg.run(inputData)).toEqual([
    { a3: 16 },
    { a3: 24 }
  ]);

})