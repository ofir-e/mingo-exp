# mingo-exp

  extend mingo with any custom function and support async code in aggregations.
  want to transform objects to objects?
  get the full power of mongo aggregation framework using mingo + the power of your own js\ts code 💪

![license](https://img.shields.io/github/license/ofir-e/mingo-exp)
[![version](https://img.shields.io/npm/v/mingo-exp)](https://www.npmjs.org/package/mingo-exp)
[![npm downloads](https://img.shields.io/npm/dm/mingo-exp)](https://www.npmjs.org/package/mingo-exp)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/ofir-e/mingo-exp/Node.js%20CI)
[![HitCount](https://hits.dwyl.com/ofir-e/mingo-exp.svg)](https://hits.dwyl.com/ofir-e/mingo-exp.svg)
[![codecov](https://img.shields.io/codecov/c/github/ofir-e/mingo-exp)](https://codecov.io/gh/ofir-e/mingo-exp)

## Installation

    npm install mingo-exp

(typescript ready, no need for @types/mingo-exp :))

## Example

~~~ javascript
import { Aggregator } from 'mingo/aggregator';
import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';

import { customParseExpression } from 'mingo-exp';

const { $customParse } = customParseExpression({ multiply3Numbers: (num1: number, num2: number, num3: number) => num1 * num2 * num3 });

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as Record<string, PipelineOperator>);
useOperators(OperatorType.EXPRESSION, { $customParse });

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
      q: { $customParse: { type: 'concat', args: ['*', '$a.b.c', '$a.d', '$a.e'] } },
      a: { $customParse: { type: 'multiply3Numbers', args: ['$a.b.c', '$a.d', '$a.e'] } }
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
import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';
import delay from 'delay';

import { generateCustomOperator, AsyncAggregator } from 'mingo-exp';

// simplified custom operator
const { $testAsync } = generateCustomOperator({
  $testAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1 + num2;
  }
});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as Record<string, PipelineOperator>);
useOperators(OperatorType.EXPRESSION, { $testAsync });

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
          a1: { $testAsync: ['$a.d', '$a.e'] },
          a2: { $testAsync: ['$a.b.c', '$a.d'] }
        }
      },
      {
        $project: {
          a3: { $testAsync: ['$a1', '$a2'] },
        }
      }
    ]);

  console.log(await agg.run(data));
}

main();
~~~

This will print:

~~~ javascript
[
  { a3: 16 },
  { a3: 24 }
]
~~~

## advanced async Example
note that ```$collect``` and ```$runOnce``` are ```EXPRESSION``` operators of this package and are compatible with ```AsyncAggregator```, ```$runOnce``` makes promises execute only once (based on current pipe args of the promise) and the response is available for every document, ```$collect``` is similar to ```$push``` but with no need of grouping

~~~ javascript
import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';
import delay from 'delay';
import _ from 'lodash';
import { AsyncAggregator, generateCustomOperator } from '../src/index';

// simplified custom operator
const { $sendToServerInBatchesForPerformance, $lodashFind } = generateCustomOperator({
  $sendToServerInBatchesForPerformance: async (ids: string[]) => {
    // in real usage this can be a request to a server
    await delay(1000);
    return ids.map(id => ({ id, name: `king number ${id}` }));
  },
  $lodashFind: (arr: any[], predicate: any, pathInMatch: string='') => {
    return _.get(_.find(arr, predicate), pathInMatch);
  }
});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as Record<string, PipelineOperator>);
useOperators(OperatorType.EXPRESSION, { $lodashFind, $sendToServerInBatchesForPerformance });

const main = async () => {
  const data = [
    { a: { b: '123' } },
    { a: { b: '321' } }
  ]

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
        name: { $lodashFind: ['$response', { id: '$id' },'name'] }
      }
    }
  ]);

  console.log(await agg.run(data));
}

main();
~~~

This will print:

~~~ javascript
[
  { id: '123', name: 'king number 123' }, 
  { id: '321', name: 'king number 321' }
]
~~~
