import { Aggregator } from 'mingo/aggregator';
import { useOperators, OperatorType } from 'mingo/core';
import { $project } from 'mingo/operators/pipeline';

import { customParseExpression } from '../src/index';

const { $customParse } = customParseExpression({ multiply3Numbers: (num1: number, num2: number, num3: number) => num1 * num2 * num3 });

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project } as any);
useOperators(OperatorType.EXPRESSION, { $customParse } as any);

const inputData = [
  {
    a: {
      d: '2018-04-04T16:00:00.000Z',
      n: '5',
      t: ' aaa a   ',
      b: 'True',
      p: { lon: 33.03925, lat: 36.9345 },
      w: ' POLYGON( ( 0 0 ,10 10  ,0 10  ,   10 0, 0  0) )  ',
      g: { 'type': 'Point', 'coordinates': [10.0, 20.0] },
      o: null,
      c: {
        s: '*', c1: '6', c2: 'a66a'
      }
    }
  },
  {
    a: {
      d: 'Feb 19 2022 13:26:28',
      n: 5,
      t: ' b   ',
      b: false,
      p: { lon: 33.03925, lat: 36.9345 },
      w: ' POINT( 10, 10 )  ',
      g: { 'type': 'Point', 'coordinates': [10.0, 20.0] },
      o: false,
      c: {
        s: ',', c1: '6', c2: 'a66a', c3: '6'
      }
    }
  }
]

test('default custom functions', function () {
  const agg = new Aggregator([
    {
      $project: {
        d: { $customParse: { type: 'date', args: ['$a.d'] } },
        n: { $customParse: { type: 'number', args: ['$a.n'] } },
        t: { $customParse: { type: 'text', args: ['$a.t'] } },
        b: { $customParse: { type: 'boolean', args: ['$a.b'] } },
        p: { $customParse: { type: 'point', args: ['$a.p.lon', '$a.p.lat'] } },
        w: { $customParse: { type: 'wkt', args: ['$a.w'] } },
        g: { $customParse: { type: 'geoJson', args: ['$a.g'] } },
        o: { $customParse: { type: 'orDefault', args: ['$a.o', 6] } },
        c: { $customParse: { type: 'concat', args: ['$a.c.s', '$a.c.c1', '$a.c.c2', '$a.c.c3', { $customParse: { type: 'boolean', args: ['$a.d'] } },{ $customParse: { type: 'boolean', args: ['$a.nope'] } },{ $customParse: { type: 'boolean', args: ['fAlse'] } }] } }
      }
    }
  ]);
  expect(agg.run(inputData)).toEqual([
    {
      b: true,
      c: '6*a66a',
      d: '2018-04-04T16:00:00.000Z',
      g: 'POINT(10 20)',
      n: 5,
      o: 6,
      p: 'POINT(33.03925 36.9345)',
      t: 'aaa a',
      w: 'GEOMETRYCOLLECTION(POLYGON((0 0,5 5,10 0,0 0)),POLYGON((5 5,10 10,0 10,5 5)))',
    },
    {
      b: false,
      c: '6,a66a,6',
      d: '2022-02-19T11:26:28.000Z',
      g: 'POINT(10 20)',
      n: 5,
      o: false,
      p: 'POINT(33.03925 36.9345)',
      t: 'b',
      w: 'POINT(10 10)',
    }
  ]);
})