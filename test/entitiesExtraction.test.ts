import { useOperators, OperatorType, PipelineOperator } from 'mingo/core';
import { $project, $group, $set } from 'mingo/operators/pipeline';
import { $map, $cond } from 'mingo/operators/expression';
import { $push } from 'mingo/operators/accumulator';
import { AsyncAggregator, generateCustomOperator, customParseExpression } from '../src/index';
import Cache from 'memory-cache';
import _ from 'lodash';
const { customFunctions } = customParseExpression();
// simplified custom operator
const customOperators = generateCustomOperator({
  $flat: (...args) => _.flatten(args),
  $wkt: customFunctions.wkt,
  $text: customFunctions.text,
  $orDefault: customFunctions.orDefault,
  $pByMih: (key) => `p${key}`,
  $addDifRelations: (...args) => _.flatten(args),
  $addDifEntities: (...args) => _.flatten(args),

});

// ensure the required operators are preloaded prior to using them.
useOperators(OperatorType.PIPELINE, { $project, $group, $set } as Record<string, PipelineOperator>);
useOperators(OperatorType.EXPRESSION, customOperators);
useOperators(OperatorType.EXPRESSION, { $map, $cond } as any);
useOperators(OperatorType.ACCUMULATOR, { $push } as any);

const inputData = [
  { action: "update", mih: "3", a: { b: { p: "lucas", wkt: "POINT(10 11)", cl: "193245", mih: "1", code: "a-123", names: ["n1", "n2"] } }, tcs: [{ a: { b: { ps: [], wkt: "POINT(10 11)", cl: "193245", mih: "1", code: "a-123 a", names: ["nn1", "nn2"] } } }] },
  { action: "update", mih: "3", a: { b: { p: "lucy", wkt: "POINT(10 11)", cl: "193245", mih: "1", code: "b-123", names: ["n1", "n2"] } }, tcs: [{ a: { b: { ps: ["lucy"], wkt: "POINT(10 11)", cl: "193245", mih: "1", code: "b-123 a", names: ["nn1", "nn2"] } } }] },
  { action: "delete", mih: "3", a: { b: { p: "lucia", wkt: "POINT(10 11)", cl: "193245", mih: "1", code: "c-123", names: ["n1", "n2"] } }, tcs: [{ a: { b: { ps: ["lucy", "lucas"], wkt: "POINT(10 11)", cl: "193245", mih: "1", code: "c-123 a", names: ["nn1", "nn2"] } } }] },
]

test('collect can be called for multiple fields ', async function () {
  const agg = new AsyncAggregator([
    {
      $project: {
        action: "$action",
        entities: {
          $flat: [
            {
              type: "t",
              fields: [{
                id: "wkt",
                values: [{ $wkt: ["$a.b.wkt"] }],
                ps: ["p1", "p2", { $pByMih: ["$mih"] }], // mixed p that is based on other field
                cl: [{ $orDefault: ["$a.b.cl", "123456"] }], // can have default like this or once in previous stages
                isTitle: false
              },
              {
                id: "code",
                values: [{ $text: ["$a.b.code"] }],
                ps: ["p1", "p2"],
                cl: [{ $orDefault: ["$a.b.cl", "123456"] }],
                isTitle: true // title field
              },
              {
                id: "names", // multiple field
                values: { $map: { input: "$a.b.names", as: "curName", in: { $text: ["$$curName"] } } },
                ps: ["p1", "p2"],
                cl: [{ $orDefault: ["$a.b.cl", "123456"] }],
                isTitle: false
              }]
            },
            {
              $map: {
                input: "$tcs", as: "curTC", in: {
                  type: "tc",
                  fields: [{
                    id: "code",
                    values: ["$$curTC.a.b.code"],
                    ps: ["p1", "p2"],
                    cl: ["$$curTC.a.b.cl"],
                    isTitle: true
                  }],
                  override: true // flag to know if needs to ovveride used in later stage
                }
              }
            }
          ]
        },
        relations: {
          $flat: [{// single relation
            id: "tToP",
            aBDT: { id: "$a.b.code", type: "code" },
            bBDT: { id: "$a.b.p", type: "p" },
            ps: ["p1"],
            cl: ["193245"]
          },
          {// multiple relation
            $map: {
              input: "$tcs", as: "curTC", in: {
                id: "tTotc",
                aBDT: { id: "$a.b.code", type: "code" },
                bBDT: { id: "$$curTC.a.b.code", type: "code" },
                ps: ["p1"],
                cl: ["193245"]
              }
            }
          },
          {// multiple entities and each have multiple relation
            $flat: {
              $map: {
                input: "$tcs", as: "curTC", in: {
                  $map: {
                    input: "$$curTC.a.b.ps", as: "curP", in: {
                      id: "tTotc",
                      aBDT: { id: "$$curTC.a.b.code", type: "code" },
                      bBDT: { id: "$$curP", type: "p" },
                      ps: ["p1"],
                      cl: ["193245"],
                      override: true // flag to know if needs to ovveride used in later stage
                    }
                  }
                }
              }
            }
          }]
        }
      }
    },
    {// at the end single doc with all relations and entities to update and delete
      $group:
      {
        update: {
          entities: { $flat: { $push: { $cond: [{ $eq: ["$action", "update"] }, "$entities", []] } } },
          relations: { $flat: { $push: { $cond: [{ $eq: ["$action", "update"] }, "$relations", []] } } }
        },
        delete: {
          entities: { $flat: { $push: { $cond: [{ $eq: ["$action", "delete"] }, "$entities", []] } } },
          relations: { $flat: { $push: { $cond: [{ $eq: ["$action", "delete"] }, "$relations", []] } } }
        }
      }
    },
    {
      $set: {
        delete: {
          entities: { $addDifEntities: ["$delete.entities"] },
          relations: { $addDifRelations: ["$delete.relations"] }
        }
      }
    }
  ]);

  const res = await agg.run(inputData);

  expect(res).toEqual([
    {
      update: {
        entities: [
          {
            type: "t",
            fields: [
              {
                id: "wkt",
                values: [
                  "POINT(10 11)",
                ],
                ps: [
                  "p1",
                  "p2",
                  "p3",
                ],
                cl: [
                  "193245",
                ],
                isTitle: false,
              },
              {
                id: "code",
                values: [
                  "a-123",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: true,
              },
              {
                id: "names",
                values: [
                  "n1",
                  "n2",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: false,
              },
            ],
          },
          {
            type: "tc",
            fields: [
              {
                id: "code",
                values: [
                  "a-123 a",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: true,
              },
            ],
            override: true,
          },
          {
            type: "t",
            fields: [
              {
                id: "wkt",
                values: [
                  "POINT(10 11)",
                ],
                ps: [
                  "p1",
                  "p2",
                  "p3",
                ],
                cl: [
                  "193245",
                ],
                isTitle: false,
              },
              {
                id: "code",
                values: [
                  "b-123",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: true,
              },
              {
                id: "names",
                values: [
                  "n1",
                  "n2",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: false,
              },
            ],
          },
          {
            type: "tc",
            fields: [
              {
                id: "code",
                values: [
                  "b-123 a",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: true,
              },
            ],
            override: true,
          },
        ],
        relations: [
          {
            id: "tToP",
            aBDT: {
              id: "a-123",
              type: "code",
            },
            bBDT: {
              id: "lucas",
              type: "p",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
          },
          {
            id: "tTotc",
            aBDT: {
              id: "a-123",
              type: "code",
            },
            bBDT: {
              id: "a-123 a",
              type: "code",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
          },
          {
            id: "tToP",
            aBDT: {
              id: "b-123",
              type: "code",
            },
            bBDT: {
              id: "lucy",
              type: "p",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
          },
          {
            id: "tTotc",
            aBDT: {
              id: "b-123",
              type: "code",
            },
            bBDT: {
              id: "b-123 a",
              type: "code",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
          },
          {
            id: "tTotc",
            aBDT: {
              id: "b-123 a",
              type: "code",
            },
            bBDT: {
              id: "lucy",
              type: "p",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
            override: true,
          },
        ],
      },
      delete: {
        entities: [
          {
            type: "t",
            fields: [
              {
                id: "wkt",
                values: [
                  "POINT(10 11)",
                ],
                ps: [
                  "p1",
                  "p2",
                  "p3",
                ],
                cl: [
                  "193245",
                ],
                isTitle: false,
              },
              {
                id: "code",
                values: [
                  "c-123",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: true,
              },
              {
                id: "names",
                values: [
                  "n1",
                  "n2",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: false,
              },
            ],
          },
          {
            type: "tc",
            fields: [
              {
                id: "code",
                values: [
                  "c-123 a",
                ],
                ps: [
                  "p1",
                  "p2",
                ],
                cl: [
                  "193245",
                ],
                isTitle: true,
              },
            ],
            override: true,
          },
        ],
        relations: [
          {
            id: "tToP",
            aBDT: {
              id: "c-123",
              type: "code",
            },
            bBDT: {
              id: "lucia",
              type: "p",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
          },
          {
            id: "tTotc",
            aBDT: {
              id: "c-123",
              type: "code",
            },
            bBDT: {
              id: "c-123 a",
              type: "code",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
          },
          {
            id: "tTotc",
            aBDT: {
              id: "c-123 a",
              type: "code",
            },
            bBDT: {
              id: "lucy",
              type: "p",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
            override: true,
          },
          {
            id: "tTotc",
            aBDT: {
              id: "c-123 a",
              type: "code",
            },
            bBDT: {
              id: "lucas",
              type: "p",
            },
            ps: [
              "p1",
            ],
            cl: [
              "193245",
            ],
            override: true,
          },
        ],
      },
    },
  ]);

})

afterAll(function () {
  Cache.clear();
})
