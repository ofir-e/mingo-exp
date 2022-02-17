import { Aggregator } from 'mingo/aggregator';
import { useOperators, OperatorType } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';

import { customParseExpression, generateCustomOperator } from '../src/index';

const { $multiply3Numbers } = generateCustomOperator({ $multiply3Numbers: (num1: number, num2: number, num3: number) => num1 * num2 * num3 });
const { $customParse } = customParseExpression();
// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as any);
useOperators(OperatorType.EXPRESSION, { $multiply3Numbers, $customParse } as any);

const inputData = [
  { a: { b: { c: 3 }, d: 4, e: 5 } },
  { a: { b: { c: 6 }, d: 6, e: 6 } }
]

test('custom function called', function () {
  const agg = new Aggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        a: { $multiply3Numbers: ['$a.b.c', '$a.d', '$a.e'] }
      }
    }
  ]);
  expect(agg.run(inputData)).toEqual([
    { n1: 3, n2: 4, n3: 5, a: 60 },
    { n1: 6, n2: 6, n3: 6, a: 216 }
  ]);
})

test('default custom function called', function () {
  const agg = new Aggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        q: { $customParse: { type: 'concat', args: ['*', '$a.b.c', '$a.d', '$a.e'] } },
      }
    }
  ]);
  expect(agg.run(inputData)).toEqual([
    { n1: 3, n2: 4, n3: 5, q: '3*4*5' },
    { n1: 6, n2: 6, n3: 6, q: '6*6*6' }
  ]);
})

test('both default and custom function called', function () {
  const agg = new Aggregator([
    {
      $project: {
        n1: '$a.b.c',
        n2: '$a.d',
        n3: '$a.e',
        q: { $customParse: { type: 'concat', args: ['*', '$a.b.c', '$a.d', '$a.e'] } },
        a: { $multiply3Numbers: ['$a.b.c', '$a.d', '$a.e'] }
      }
    }
  ]);

  expect(agg.run(inputData)).toEqual([
    { n1: 3, n2: 4, n3: 5, q: '3*4*5', a: 60 },
    { n1: 6, n2: 6, n3: 6, q: '6*6*6', a: 216 }
  ]);

})