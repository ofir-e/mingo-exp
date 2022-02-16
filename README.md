# mingo-exp

    extend mingo with any custom functions with expression.
    want to transform objects to objects?
    get the full power of mongo aggregation framework using mingo + the power of your own js\ts code 💪

## Installation

    npm install mingo-exp

(typescript ready, no need for @types/mingo-exp :))

## Example

~~~ javascript
import { Aggregator } from "mingo/aggregator";
import { useOperators, OperatorType } from "mingo/core";
import { $project } from "mingo/operators/pipeline";

import { customParseExpression } from 'mingo-exp';

const { $customParse } = customParseExpression({ multiply3Numbers: (num1: number, num2: number, num3: number) => num1 * num2 * num3 });

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as any);
useOperators(OperatorType.EXPRESSION, { $customParse } as any);

const data = [
  { a: { b: { c: 3 }, d: 4, e: 5 } },
  { a: { b: { c: 6 }, d: 6, e: 6 } }
]

let agg = new Aggregator([
  {
    $project: {
      n1: '$a.b.c',
      n2: '$a.d',
      n3: '$a.e',
      q: { $customParse: { type: "concat", args: ['*', '$a.b.c', '$a.d', '$a.e'] } },
      a: { $customParse: { type: "multiply3Numbers", args: ['$a.b.c', '$a.d', '$a.e'] } }
    }
  },
]);

console.log(agg.run(data));
~~~

This will print:

~~~ javascript
[
  { n1: 3, n2: 4, n3: 5, q: '3*4*5', a: 60 },
  { n1: 6, n2: 6, n3: 6, q: '6*6*6', a: 216 }
]
~~~

## async Example

~~~ javascript
import { useOperators, OperatorType } from "mingo/core";
import { $project } from "mingo/operators/pipeline";

import { generateCustomOperator, AsyncAggregator } from 'mingo-exp';

// simplified custom operator
const { $testAsync } = generateCustomOperator({
  $testAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1 + num2;
  }
});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as any);
useOperators(OperatorType.EXPRESSION, { $testAsync } as any);

const main = async ()=>{
  const data = [
    { a: { b: { c: 3 }, d: 4, e: 5 } },
    { a: { b: { c: 6 }, d: 6, e: 6 } }
  ]

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

  console.log(await agg.run(data));
}
~~~

This will print:

~~~ javascript
[
  { a3: 16 },
  { a3: 24 }
]
~~~
